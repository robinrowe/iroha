/**
 * Copyright Soramitsu Co., Ltd. 2017 All Rights Reserved.
 * http://soramitsu.co.jp
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "torii/query_service.hpp"
#include "backend/protobuf/query_responses/proto_block_query_response.hpp"
#include "backend/protobuf/query_responses/proto_query_response.hpp"
#include "builders/default_builders.hpp"
#include "builders/protobuf/common_objects/proto_account_builder.hpp"
#include "builders/protobuf/queries.hpp"
#include "main/server_runner.hpp"
#include "module/irohad/torii/torii_mocks.hpp"
#include "module/shared_model/builders/protobuf/test_query_builder.hpp"
#include "module/shared_model/builders/protobuf/test_query_response_builder.hpp"
#include "torii/query_client.hpp"
#include "utils/query_error_response_visitor.hpp"

using namespace torii;

using namespace iroha;
using namespace iroha::torii;

using namespace shared_model::detail;
using namespace shared_model::interface;
using ::testing::_;
using ::testing::A;
using ::testing::An;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::Truly;

class QueryServiceTest : public ::testing::Test {
 public:
  void SetUp() override {
    query_processor = std::make_shared<MockQueryProcessor>();
    // any query
    query = std::make_shared<shared_model::proto::Query>(
        shared_model::proto::QueryBuilder()
            .creatorAccountId("user@domain")
            .createdTime(iroha::time::now())
            .queryCounter(1)
            .getAccount("user@domain")
            .build()
            .signAndAddSignature(
                shared_model::crypto::DefaultCryptoAlgorithmType::
                    generateKeypair())
            .finish());

    auto account = shared_model::proto::AccountBuilder()
                       .accountId("a")
                       .domainId("ru")
                       .quorum(2)
                       .build();

    model_response = clone(TestQueryResponseBuilder()
                               .accountResponse(account, {"user"})
                               .queryHash(query->hash())
                               .build());
  }

  void init() {
    query_service = std::make_shared<QueryService>(query_processor);
  }

  std::shared_ptr<shared_model::proto::Query> query;
  std::shared_ptr<shared_model::interface::BlocksQuery> blocks_query;
  std::shared_ptr<shared_model::proto::QueryResponse> model_response;
  std::shared_ptr<shared_model::interface::BlockQueryResponse> block_response;

  iroha::protocol::Block block;

  std::shared_ptr<QueryService> query_service;
  std::shared_ptr<MockQueryProcessor> query_processor;
};

/**
 * @given query and expected valid response
 * @when query is sent to query service and query_processor processes query
 * @then expected response is returned
 */
TEST_F(QueryServiceTest, ValidWhenUniqueHash) {
  // unique query => query handled by query processor
  rxcpp::subjects::subject<
      std::shared_ptr<shared_model::interface::QueryResponse>>
      notifier;
  EXPECT_CALL(*query_processor, queryNotifier())
      .WillOnce(Return(notifier.get_observable()));
  EXPECT_CALL(
      *query_processor,
      queryHandle(
          // match by shared_ptr's content
          Truly([this](std::shared_ptr<shared_model::interface::Query> rhs) {
            return *rhs == *query;
          })))
      .WillOnce(Invoke([this, &notifier](auto q) {
        notifier.get_subscriber().on_next(model_response);
      }));
  init();

  protocol::QueryResponse response;
  query_service->Find(query->getTransport(), response);
  auto resp = shared_model::proto::QueryResponse(response);
  ASSERT_EQ(resp, *model_response);
}

/**
 * @given query and expected response
 * @when query is sent to query service and query_processor does not process
 * query
 * @then NOT_SUPPORTED error response is returned
 */
TEST_F(QueryServiceTest, InvalidWhenUniqueHash) {
  // unique query => query handled by query processor
  rxcpp::subjects::subject<
      std::shared_ptr<shared_model::interface::QueryResponse>>
      notifier;
  EXPECT_CALL(*query_processor, queryNotifier())
      .WillOnce(Return(notifier.get_observable()));
  EXPECT_CALL(
      *query_processor,
      queryHandle(
          // match by shared_ptr's content
          Truly([this](std::shared_ptr<shared_model::interface::Query> rhs) {
            return *rhs == *query;
          })))
      .WillOnce(Return());
  init();

  protocol::QueryResponse response;
  query_service->Find(query->getTransport(), response);
  ASSERT_TRUE(response.has_error_response());
  auto resp = shared_model::proto::QueryResponse(response);
  ASSERT_TRUE(boost::apply_visitor(
      shared_model::interface::QueryErrorResponseChecker<
          shared_model::interface::NotSupportedErrorResponse>(),
      resp.get()));
}

/**
 * @given query
 * @when query is sent to query service twice
 * @then query processor will be invoked once and second response will have
 * STATELESS_INVALID status
 */
TEST_F(QueryServiceTest, InvalidWhenDuplicateHash) {
  // two same queries => only first query handled by query processor
  EXPECT_CALL(*query_processor, queryNotifier())
      .WillOnce(
          Return(rxcpp::observable<>::empty<std::shared_ptr<QueryResponse>>()));
  EXPECT_CALL(*query_processor, queryHandle(_)).WillOnce(Return());

  init();

  protocol::QueryResponse response;
  query_service->Find(query->getTransport(), response);

  // second call of the same query
  query_service->Find(query->getTransport(), response);
  ASSERT_TRUE(response.has_error_response());
  auto resp = shared_model::proto::QueryResponse(response);
  ASSERT_TRUE(boost::apply_visitor(
      shared_model::interface::QueryErrorResponseChecker<
          shared_model::interface::StatelessFailedErrorResponse>(),
      resp.get()));
}

class BlocksQueryServiceTest : public ::testing::Test {
 public:
  virtual void SetUp() {
    runner = std::make_unique<ServerRunner>(ip + ":0");

    // ----------- Command Service --------------
    query_processor = std::make_shared<MockQueryProcessor>();
    EXPECT_CALL(*query_processor, queryNotifier())
        .WillOnce(
            Return(rxcpp::observable<>::empty<
                   std::shared_ptr<shared_model::interface::QueryResponse>>()));

    block.mutable_payload()->set_height(123);
    auto proto_block = std::make_shared<shared_model::proto::Block>(block);

    block_response = shared_model::builder::DefaultBlockQueryResponseBuilder()
                         .blockResponse(*proto_block)
                         .build();

    //----------- Server run ----------------
    runner->append(std::make_unique<::torii::QueryService>(query_processor))
        .run()
        .match(
            [this](iroha::expected::Value<int> port) {
              this->port = port.value;
            },
            [](iroha::expected::Error<std::string> err) {
              FAIL() << err.error;
            });

    runner->waitForServersReady();
  }

  std::unique_ptr<ServerRunner> runner;
  std::shared_ptr<MockQueryProcessor> query_processor;

  rxcpp::subjects::subject<std::shared_ptr<shared_model::interface::Proposal>>
      prop_notifier_;
  std::shared_ptr<shared_model::proto::QueryResponse> model_response;
  std::shared_ptr<shared_model::interface::BlockQueryResponse> block_response;
  iroha::protocol::Block block;

  shared_model::crypto::Keypair keypair =
      shared_model::crypto::DefaultCryptoAlgorithmType::generateKeypair();

  const std::string ip = "127.0.0.1";
  int port;
};

/**
 * @given valid blocks query
 * @when blocks query is executed
 * @then valid blocks response is received and contains block emitted by query
 * processor
 */
TEST_F(BlocksQueryServiceTest, FetchBlocksWhenValidQuery) {
  auto blocks_query = std::make_shared<shared_model::proto::BlocksQuery>(
      shared_model::proto::BlocksQueryBuilder()
          .creatorAccountId("user@domain")
          .createdTime(iroha::time::now())
          .queryCounter(1)
          .build()
          .signAndAddSignature(
              shared_model::crypto::DefaultCryptoAlgorithmType::
                  generateKeypair())
          .finish());

  EXPECT_CALL(
      *query_processor,
      blocksQueryHandle(
          Truly([&blocks_query](
                    const std::shared_ptr<shared_model::interface::BlocksQuery>
                        query) { return *query == *blocks_query; })))
      .WillOnce(Return(rxcpp::observable<>::just(block_response)));

  auto client = torii_utils::QuerySyncClient(ip, port);
  std::vector<iroha::protocol::BlockQueryResponse> responses;
  auto proto_blocks_query =
      std::static_pointer_cast<shared_model::proto::BlocksQuery>(blocks_query);
  client.FetchCommits(proto_blocks_query->getTransport(), responses);

  ASSERT_EQ(responses.size(), 1);
  auto response = responses.at(0);
  ASSERT_TRUE(response.has_block_response());
  ASSERT_EQ(response.block_response().block().SerializeAsString(),
            block.SerializeAsString());
}

/**
 * @given stateless invalid blocks query
 * @when blocks query is executed
 * @then block error response is received
 */
TEST_F(BlocksQueryServiceTest, FetchBlocksWhenInvalidQuery) {
  EXPECT_CALL(*query_processor, blocksQueryHandle(_)).Times(0);

  auto blocks_query = std::make_shared<shared_model::proto::BlocksQuery>(
      TestUnsignedBlocksQueryBuilder()
          .creatorAccountId("asd@@domain")  // invalid account id name
          .createdTime(iroha::time::now())
          .queryCounter(1)
          .build()
          .signAndAddSignature(
              shared_model::crypto::DefaultCryptoAlgorithmType::
                  generateKeypair())
          .finish());

  auto client = torii_utils::QuerySyncClient(ip, port);
  std::vector<iroha::protocol::BlockQueryResponse> responses;
  auto proto_blocks_query =
      std::static_pointer_cast<shared_model::proto::BlocksQuery>(blocks_query);
  client.FetchCommits(proto_blocks_query->getTransport(), responses);

  ASSERT_EQ(responses.size(), 1);
  auto response = responses.at(0);
  ASSERT_TRUE(response.has_error_response());
}

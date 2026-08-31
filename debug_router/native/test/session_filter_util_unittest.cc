// Copyright 2026 The Lynx Authors. All rights reserved.
// Licensed under the Apache License Version 2.0 that can be found in the
// LICENSE file in the root directory of this source tree.

#include "debug_router/native/core/session_filter_util.h"

#include "gtest/gtest.h"

namespace debugrouter {
namespace core {
namespace internal {
namespace {

TEST(SessionFilterUtilTest, ExtractSessionIdFromCustomizedPayload) {
  const std::string message = R"({
    "event":"Customized",
    "data":{
      "type":"CDP",
      "sender":1,
      "data":{
        "session_id":42,
        "client_id":1,
        "message":"{}"
      }
    }
  })";

  int32_t session_id = -1;
  EXPECT_TRUE(ExtractIncomingSessionIdForFilter(message, session_id));
  EXPECT_EQ(session_id, 42);
}

TEST(SessionFilterUtilTest, IgnoreSessionListControlMessage) {
  const std::string message = R"({
    "event":"Customized",
    "data":{
      "type":"SessionList",
      "sender":1,
      "data":[
        {"session_id":42,"url":"test","type":"lynx"}
      ]
    }
  })";

  int32_t session_id = -1;
  EXPECT_FALSE(ExtractIncomingSessionIdForFilter(message, session_id));
  EXPECT_EQ(session_id, -1);
}

TEST(SessionFilterUtilTest, IgnoreNegativeSessionId) {
  const std::string message = R"({
    "event":"Customized",
    "data":{
      "type":"CDP",
      "sender":1,
      "data":{
        "session_id":-1,
        "client_id":1,
        "message":"{}"
      }
    }
  })";

  int32_t session_id = -1;
  EXPECT_FALSE(ExtractIncomingSessionIdForFilter(message, session_id));
  EXPECT_EQ(session_id, -1);
}

TEST(SessionFilterUtilTest, IgnoreZeroSessionId) {
  const std::string message = R"({
    "event":"Customized",
    "data":{
      "type":"CDP",
      "sender":1,
      "data":{
        "session_id":0,
        "client_id":1,
        "message":"{}"
      }
    }
  })";

  int32_t session_id = -1;
  EXPECT_FALSE(ExtractIncomingSessionIdForFilter(message, session_id));
  EXPECT_EQ(session_id, 0);
}

TEST(SessionFilterUtilTest, IgnoreStringLiteralSessionIdInMessageBody) {
  const std::string message = R"({
    "event":"Customized",
    "data":{
      "type":"CDP",
      "sender":1,
      "data":{
        "client_id":1,
        "message":"WARN: \"session_id\":42 missing"
      }
    }
  })";

  int32_t session_id = -1;
  EXPECT_FALSE(ExtractIncomingSessionIdForFilter(message, session_id));
  EXPECT_EQ(session_id, -1);
}

TEST(SessionFilterUtilTest, IgnoreNonNumericSessionId) {
  const std::string message = R"({
    "event":"Customized",
    "data":{
      "type":"CDP",
      "sender":1,
      "data":{
        "session_id":"42",
        "client_id":1,
        "message":"{}"
      }
    }
  })";

  int32_t session_id = -1;
  EXPECT_FALSE(ExtractIncomingSessionIdForFilter(message, session_id));
  EXPECT_EQ(session_id, -1);
}

}  // namespace
}  // namespace internal
}  // namespace core
}  // namespace debugrouter

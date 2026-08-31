// Copyright 2025 The Lynx Authors. All rights reserved.
// Licensed under the Apache License Version 2.0 that can be found in the
// LICENSE file in the root directory of this source tree.

#include "debug_router/native/net/websocket_task.h"

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

namespace debugrouter {
namespace net {
namespace {

class FakeMessageTransceiver final : public core::MessageTransceiver {
 public:
  bool Connect(const std::string& url) override { return true; }
  void Disconnect() override {}
  void Send(const std::string& data) override {}
  core::ConnectionType GetType() override {
    return core::ConnectionType::kWebSocket;
  }
  void StartServer() override {}
  void StopServer() override {}
};

class CountingDelegate final : public core::MessageTransceiverDelegate {
 public:
  void OnOpen(
      const std::shared_ptr<core::MessageTransceiver>& transceiver) override {}

  void OnClosed(
      const std::shared_ptr<core::MessageTransceiver>& transceiver) override {
    closed_count.fetch_add(1, std::memory_order_relaxed);
  }

  void OnFailure(const std::shared_ptr<core::MessageTransceiver>& transceiver,
                 const std::string& error_message, int error_code) override {
    failure_count.fetch_add(1, std::memory_order_relaxed);
  }

  void OnMessage(
      const std::string& message,
      const std::shared_ptr<core::MessageTransceiver>& transceiver) override {}

  void OnInit(const std::shared_ptr<core::MessageTransceiver>& transceiver,
              int32_t code, const std::string& info) override {}

  std::atomic<int> closed_count{0};
  std::atomic<int> failure_count{0};
};

TEST(WebSocketTaskTestSuite, ConcurrentStopOnlyNotifiesCloseOnce) {
  auto transceiver = std::make_shared<FakeMessageTransceiver>();
  CountingDelegate delegate;
  transceiver->SetDelegate(&delegate);

  auto task = std::make_unique<WebSocketTask>(transceiver, "ws://127.0.0.1");
  task->SetConnectedForTest(true);

  std::vector<std::thread> threads;
  for (int i = 0; i < 4; ++i) {
    threads.emplace_back([&task]() { task->Stop(); });
  }
  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(delegate.closed_count.load(std::memory_order_relaxed), 1);
  EXPECT_EQ(delegate.failure_count.load(std::memory_order_relaxed), 0);
}

TEST(WebSocketTaskTestSuite, StopBeforeOpenDoesNotNotifyClose) {
  auto transceiver = std::make_shared<FakeMessageTransceiver>();
  CountingDelegate delegate;
  transceiver->SetDelegate(&delegate);

  auto task = std::make_unique<WebSocketTask>(transceiver, "ws://127.0.0.1");

  task->Stop();

  EXPECT_EQ(delegate.closed_count.load(std::memory_order_relaxed), 0);
  EXPECT_EQ(delegate.failure_count.load(std::memory_order_relaxed), 0);
}

TEST(WebSocketTaskTestSuite, FailureBeforeOpenDoesNotNotifyClose) {
  auto transceiver = std::make_shared<FakeMessageTransceiver>();
  CountingDelegate delegate;
  transceiver->SetDelegate(&delegate);

  auto task = std::make_unique<WebSocketTask>(transceiver, "ws://127.0.0.1");

  task->NotifyFailureForTest("connect failed", -1);

  EXPECT_EQ(delegate.failure_count.load(std::memory_order_relaxed), 1);
  EXPECT_EQ(delegate.closed_count.load(std::memory_order_relaxed), 0);
}

TEST(WebSocketTaskTestSuite, FailureAlsoNotifiesCloseOnce) {
  auto transceiver = std::make_shared<FakeMessageTransceiver>();
  CountingDelegate delegate;
  transceiver->SetDelegate(&delegate);

  auto task = std::make_unique<WebSocketTask>(transceiver, "ws://127.0.0.1");
  task->SetConnectedForTest(true);

  task->NotifyFailureForTest("send failed", -1);

  EXPECT_EQ(delegate.failure_count.load(std::memory_order_relaxed), 1);
  EXPECT_EQ(delegate.closed_count.load(std::memory_order_relaxed), 1);

  task->NotifyCloseForTest();
  task->NotifyFailureForTest("send failed again", -2);

  EXPECT_EQ(delegate.failure_count.load(std::memory_order_relaxed), 1);
  EXPECT_EQ(delegate.closed_count.load(std::memory_order_relaxed), 1);
}

TEST(WebSocketTaskTestSuite, ConcurrentStopAndFailureCloseOnlyOnce) {
  auto transceiver = std::make_shared<FakeMessageTransceiver>();
  CountingDelegate delegate;
  transceiver->SetDelegate(&delegate);

  auto task = std::make_unique<WebSocketTask>(transceiver, "ws://127.0.0.1");
  task->SetConnectedForTest(true);

  std::thread stop_thread([&task]() { task->Stop(); });
  std::thread failure_thread(
      [&task]() { task->NotifyFailureForTest("send failed", -1); });

  stop_thread.join();
  failure_thread.join();

  const int failure_count =
      delegate.failure_count.load(std::memory_order_relaxed);
  EXPECT_EQ(delegate.closed_count.load(std::memory_order_relaxed), 1);
  EXPECT_GE(failure_count, 0);
  EXPECT_LE(failure_count, 1);
}

}  // namespace
}  // namespace net
}  // namespace debugrouter

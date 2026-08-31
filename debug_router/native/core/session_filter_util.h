// Copyright 2025 The Lynx Authors. All rights reserved.
// Licensed under the Apache License Version 2.0 that can be found in the
// LICENSE file in the root directory of this source tree.

#ifndef DEBUGROUTER_NATIVE_CORE_SESSION_FILTER_UTIL_H_
#define DEBUGROUTER_NATIVE_CORE_SESSION_FILTER_UTIL_H_

#include <cstdint>
#include <string>
#include <string_view>

namespace debugrouter {
namespace core {
namespace internal {

bool ExtractIncomingSessionIdForFilter(const std::string &message,
                                       int32_t &session_id);

bool ShouldDropIncomingBySessionFilter(const std::string &message,
                                       std::string_view transport_name);

}  // namespace internal
}  // namespace core
}  // namespace debugrouter

#endif  // DEBUGROUTER_NATIVE_CORE_SESSION_FILTER_UTIL_H_

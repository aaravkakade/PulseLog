// A blocking one-shot HTTP GET, used to test the metrics endpoint.
#ifndef PULSELOG_TESTS_TEST_SUPPORT_HTTP_CLIENT_H_
#define PULSELOG_TESTS_TEST_SUPPORT_HTTP_CLIENT_H_

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cstdint>
#include <string>

namespace pulselog::testing {

// Returns the raw response (status line, headers and body), or an empty string
// if the request failed.
inline std::string HttpGetRaw(std::uint16_t port, const std::string& path,
                              const std::string& host = "127.0.0.1") {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return {};

  ::sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
    ::close(fd);
    return {};
  }
  if (::connect(fd, reinterpret_cast<::sockaddr*>(&addr), sizeof(addr)) != 0) {
    ::close(fd);
    return {};
  }

  const std::string request =
      "GET " + path + " HTTP/1.1\r\nHost: " + host + "\r\nConnection: close\r\n\r\n";
  std::size_t sent = 0;
  while (sent < request.size()) {
    const ssize_t wrote = ::send(fd, request.data() + sent, request.size() - sent, 0);
    if (wrote <= 0) break;
    sent += static_cast<std::size_t>(wrote);
  }

  std::string response;
  std::array<char, 4096> buffer{};
  for (;;) {
    const ssize_t got = ::recv(fd, buffer.data(), buffer.size(), 0);
    if (got <= 0) break;
    response.append(buffer.data(), static_cast<std::size_t>(got));
  }
  ::close(fd);
  return response;
}

}  // namespace pulselog::testing

#endif  // PULSELOG_TESTS_TEST_SUPPORT_HTTP_CLIENT_H_

namespace java com.xiaomi.linden.thrift.common

struct Response {
    1: required bool success = 1,
    2: optional string error,
}
@0xdca578873284febb;

struct GetRequest {
  key @0 :Text;
}

struct GetResponse {
  value @0 :Text;
}

struct PutRequest {
  key @0 :Text;
  value @1 :Text;
}

struct PutResponse {
}

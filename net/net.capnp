@0xdca578873284febb;

struct Request {
  id @0 :UInt64;

  union {
    get :group {
      key @1 :Text;
    }
    put :group {
      key @2 :Text;
      value @3 :Text;
    }
  }
}

struct Response {
  id @0 :UInt64;

  union {
    result :group {
      value @1 :Text;
    }
    ok :group {
      ok @2 :Void;
    }
    error :group {
      message @3 :Text;
    }
  }
}

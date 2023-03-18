@0xdca578873284febb;

struct Request {
  union {
    get :group {
      key @0 :Text;
    }
    put :group {
      key @1 :Text;
      value @2 :Text;
    }
  }
}

struct Response {
  union {
    result :group {
      value @0 :Text;
    }
    ok :group {
      ok @1 :Void;
    }
    error :group {
      message @2 :Text;
    }
  }
}

@0xbb123cbc36094a50;

struct Request {
  requestId @0 :Text;

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
  requestId @0 :Text;

  union {
    result :group {
      value @1 :Text;
      dataId @2 :Text;
    }
    ok :group {
      ok @3 :Void;
    }
    error :group {
      message @4 :Text;
    }
  }
}

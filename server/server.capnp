@0xbb123cbc36094a50;

struct Request {
  requestId @0 :Text;

  union {
    get :group {
      key @1 :Text;
    }
    digest :group {
      key @2 :Text;
    }
    put :group {
      key @3 :Text;
      value @4 :Text;
    }
    delete :group {
      key @5 :Text;
    }
  }
}

struct Response {
  requestId @0 :Text;

  union {
    result :group {
      dataId @1 :Text;
      digest @2 :UInt64;
      value @3 :Text;
    }
    ok :group {
      ok @4 :Void;
    }
    error :group {
      message @5 :Text;
    }
  }
}

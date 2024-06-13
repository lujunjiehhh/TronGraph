function i(e) {
  return function () {
    var t = this,
      n = arguments;
    return new Promise(function (r, i) {
      var a = e.apply(t, n);
      function c(e) {
        o(a, r, i, c, u, "next", e);
      }
      function u(e) {
        o(a, r, i, c, u, "throw", e);
      }
      c(void 0);
    });
  };
}

_e = function () {
  var e = i(
    r().mark(function e(t) {
      var n, o, i, a, c, u, s, l, f, d, p;
      return r().wrap(function (e) {
        for (;;)
          switch ((e.prev = e.next)) {
            case 0:
              return (
                (n = t.token),
                (o = t.timestampForTest),
                (i = ""),
                (e.next = 4),
                crypto.subtle.digest("SHA-256", new TextEncoder().encode(n))
              );
            case 4:
              for (
                a = e.sent,
                  c = Array.from(new Uint8Array(a)),
                  u = c
                    .map(function (e) {
                      return e.toString(16).padStart(2, "0");
                    })
                    .join(""),
                  s = o || Math.floor(Date.now() / 1e3),
                  l = Math.floor((s / 600) % 32),
                  f = Math.floor((s / 3600) % 32),
                  d = 0;
                d < 32;
                d++
              )
                (p = u[(l + (f + d) * d) % 32]), (i += p);
              return e.abrupt("return", i);
            case 12:
            case "end":
              return e.stop();
          }
      }, e);
    })
  );
  return function (t) {
    return e.apply(this, arguments);
  };
};
function callFunction() {
  _e({ token: "your-token-here", timestampForTest: 1625140800 })
    .then((result) => {
      console.log(result);
    })
    .catch((error) => {
      console.error("Error:", error);
    });
}

callFunction();

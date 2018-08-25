let timers = {}

function main() {
  let Echo = zeno.examples.js.Echo;

  let snap = Snap('#svg');
  nodes = {};
  nodes[Echo.server.address] = snap.circle(200, 100, 20);
  nodes[Echo.clientA.address] = snap.circle(100, 300, 20);
  nodes[Echo.clientB.address] = snap.circle(200, 300, 20);
  nodes[Echo.clientC.address] = snap.circle(300, 300, 20);

  function config_message(app, message) {
    let src = nodes[message.src];
    let dst = nodes[message.dst];
    return {
      svg_message: snap.circle(src.attr("cx"), src.attr("cy"), 10),
      animate_args: {cx: dst.attr("cx"), cy: dst.attr("cy")},
      timeout: 350 + Math.random() * 100,
      drop: Math.random() <= 0.1,
    };
  }

  function on_timer_stop(timer) {}

  function on_timer_start(timer) {}

  let app = new z.SimulatedApp(Echo.transport, {
    config_message: config_message,
    on_timer_stop: on_timer_stop,
    on_timer_start: on_timer_start,
  });
}

window.onload = main

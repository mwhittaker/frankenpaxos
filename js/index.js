let timers = {}

function main() {
  let Echo = zeno.examples.js.Echo;

  let snap = Snap('#svg');
  nodes = {};
  nodes[Echo.server.address] = snap.circle(200, 100, 20);
  nodes[Echo.clientA.address] = snap.circle(100, 300, 20);
  nodes[Echo.clientB.address] = snap.circle(200, 300, 20);
  nodes[Echo.clientC.address] = snap.circle(300, 300, 20);

  function on_send(app, message) {
    let timeout = 100 + (Math.random() * 350);
    // console.log('Delaying message by ' + timeout);

    let src = nodes[message.src];
    let dst = nodes[message.dst];
    let msg = snap.circle(src.attr("cx"), src.attr("cy"), 10);
    msg.animate({cx: dst.attr("cx"), cy: dst.attr("cy")}, timeout, () => {
      msg.remove();
      app.transport.deliverMessage(message);
      app.refresh();
    });

    // setTimeout(() => {
    //   app.transport.deliverMessage(message);
    //   app.refresh();
    // }, timeout);
  }

  function on_timer_stop(app, timer) {
    clearTimeout(timers[timer.address][timer.name()]);
  }

  function on_timer_start(app, timer) {
    if (!(timer.address in timers)) {
      timers[timer.address] = {}
    }

    timers[timer.address][timer.name()] = setTimeout(() => {
      timer.stop();
      timer.run();
      app.refresh();
    }, timer.delayMilliseconds());
  }

  let app = new z.App(Echo.transport, {
    on_send: on_send,
    on_timer_stop: on_timer_stop,
    on_timer_start: on_timer_start,
  });
  app.refresh();
  app.refresh();
}

window.onload = main

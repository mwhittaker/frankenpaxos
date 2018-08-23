let timers = {}

function on_send(app, message) {
  let timeout = 500 + (Math.random() * 1000);
  // console.log('Delaying message by ' + timeout);
  setTimeout(() => {
    app.transport.deliverMessage(message);
    app.refresh();
  }, timeout);
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

function main() {
  let Echo = zeno.examples.js.Echo;

  let app = new z.App(Echo.transport, {
    on_send: on_send,
    on_timer_stop: on_timer_stop,
    on_timer_start: on_timer_start,
  });
  app.refresh();
  app.refresh();
}

window.onload = main

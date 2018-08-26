function main() {
  let Echo = zeno.examples.js.Echo;
  let loggers = {};
  loggers[Echo.server.address] = Echo.serverLogger;
  loggers[Echo.clientA.address] = Echo.loggerA;
  loggers[Echo.clientB.address] = Echo.loggerB;
  loggers[Echo.clientC.address] = Echo.loggerC;

  let snap = Snap('#animation');
  let nodes = {};
  nodes[Echo.server.address] = snap.circle(150, 50, 15);
  nodes[Echo.clientA.address] = snap.circle(50, 150, 15);
  nodes[Echo.clientB.address] = snap.circle(150, 150, 15);
  nodes[Echo.clientC.address] = snap.circle(250, 150, 15);

  let vue_nodes = {};
  vue_nodes[Echo.server.address] = {name: 'server', log: []};
  vue_nodes[Echo.clientA.address] = {name: 'clientA', log: []};
  vue_nodes[Echo.clientB.address] = {name: 'clientB', log: []};
  vue_nodes[Echo.clientC.address] = {name: 'clientC', log: []};
  let vue_app = new Vue({
    el: '#app',
    data: {
      nodes: Object.values(vue_nodes),
    }
  });

  let simulated_app = new zenojs.SimulatedApp(Echo.transport, {
    config_message: (app, message) => {
      let src = nodes[message.src];
      let dst = nodes[message.dst];
      let svg_message = snap.circle(src.attr("cx"), src.attr("cy"), 9);
      svg_message.attr({fill: 'red'});
      snap.prepend(svg_message);
      return {
        svg_message: svg_message,
        animate_args: {cx: dst.attr("cx"), cy: dst.attr("cy")},
        timeout: 350 + Math.random() * 100,
        drop: Math.random() <= 0.1,
      };
    },
    on_send: (app, message) => {},
    on_deliver: (app, message) => {
      for (let l of loggers[message.dst].bufferedLogsJs()) {
        vue_nodes[message.dst].log.unshift(l);
      }
      loggers[message.dst].clearBufferedLogs();
    },
    on_timer_stop: (app, timer) => {},
    on_timer_start: (app, timer) => {},
  });
}

window.onload = main

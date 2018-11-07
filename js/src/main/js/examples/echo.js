function simulated_app() {
  let Echo = zeno.examples.js.SimulatedEcho.Echo;
  let snap = Snap('#simulated_animation');

  // Create nodes.
  let nodes = {};
  nodes[Echo.server.address] = {
    actor: Echo.server,
    svg: snap.circle(150, 50, 20).attr(
        {fill: '#e74c3c', stroke: 'black', 'stroke-width': '3pt'}),
  };
  nodes[Echo.clientA.address] = {
    actor: Echo.clientA,
    svg: snap.circle(75, 150, 20).attr(
        {fill: '#3498db', stroke: 'black', 'stroke-width': '3pt'}),
  };
  nodes[Echo.clientB.address] = {
    actor: Echo.clientB,
    svg: snap.circle(225, 150, 20).attr(
        {fill: '#2ecc71', stroke: 'black', 'stroke-width': '3pt'}),
  };

  // Add node titles.
  snap.text(150, 20, 'Server').attr({'text-anchor': 'middle'});
  snap.text(75, 190, 'Client A').attr({'text-anchor': 'middle'});
  snap.text(225, 190, 'Client B').attr({'text-anchor': 'middle'});

  // Create the vue app.
  let vue_app = new Vue({
    el: '#simulated_app',
    data: {
      node: nodes[Echo.server.address],
      transport: Echo.transport,
      send_message: (message, callback) => {
        let src = nodes[message.src];
        let dst = nodes[message.dst];
        let svg_message =
          snap.circle(src.svg.attr("cx"), src.svg.attr("cy"), 9)
              .attr({fill: '#2c3e50'});
        snap.prepend(svg_message);
        svg_message.animate(
          {cx: dst.svg.attr("cx"), cy: dst.svg.attr("cy")},
          250 + Math.random() * 200,
          callback);
      }
    },
  });


  // Select a node by clicking it.
  for (let node of Object.values(nodes)) {
    node.svg.node.onclick = () => {
      vue_app.node = node;
    }
  }
}

class ClickthroughVueNode {
  constructor(name, num_messages_received, log, timers, messages) {
    this.name = name;
    this.num_messages_received = num_messages_received || 0;
    this.log = log || [];
    this.timers = timers || {};
    this.messages = messages || {};
  }
}

function clickthrough_app() {
  let Echo = zeno.examples.js.ClickthroughEcho.Echo;
  let snap = Snap('#clickthrough_animation');

  // Create nodes.
  let node = (info) => {
    return {
      actor: info.actor,
      name: info.name,
      svg: info.svg,
      num_messages_received: 0,
      log: [],
      timers: [],
      messages: [],
    };
  }
  let nodes = {};
  nodes[Echo.server.address] = node({
    actor: Echo.server,
    name: 'Server',
    svg: snap.circle(150, 50, 20).attr(
        {fill: '#e74c3c', stroke: 'black', 'stroke-width': '3pt'}),
  });
  nodes[Echo.clientA.address] = node({
    actor: Echo.clientA,
    name: 'Client A',
    svg: snap.circle(75, 150, 20).attr(
        {fill: '#3498db', stroke: 'black', 'stroke-width': '3pt'}),
  });
  nodes[Echo.clientB.address] = node({
    actor: Echo.clientB,
    name: 'Client B',
    svg: snap.circle(225, 150, 20).attr(
        {fill: '#2ecc71', stroke: 'black', 'stroke-width': '3pt'}),
  });

  // Add node titles.
  snap.text(150, 20, 'Server').attr({'text-anchor': 'middle'});
  snap.text(75, 190, 'Client A').attr({'text-anchor': 'middle'});
  snap.text(225, 190, 'Client B').attr({'text-anchor': 'middle'});

  let clickthrough_app = new zenojs.ClickthroughApp(Echo.transport, {
    config_message: (app, message) => {
      let src = nodes[message.src];
      let dst = nodes[message.dst];
      let svg_message = snap.circle(src.svg.attr("cx"), src.svg.attr("cy"), 9)
                            .attr({fill: '#2c3e50'});
      snap.prepend(svg_message);
      return {
        svg_message: svg_message,
        animate_args: {cx: dst.svg.attr("cx"), cy: dst.svg.attr("cy")},
        timeout: 200,
        drop: false,
      };
    },
    on_send: (app, message) => {},
    on_receive: (app, message) => {
      nodes[message.dst].messages = app.messages[message.dst];
    },
    on_deliver: (app, message) => {
      let node = nodes[message.dst];

      // Update num_messages_received.
      node.num_messages_received = node.actor.numMessagesReceived;

      // Update log.
      for (let log_entry of node.actor.logger.bufferedLogsJs()) {
        node.log.push(log_entry);
      }
      node.actor.logger.clearBufferedLogs();

      // Update messages.
      node.messages = app.messages[message.dst];
    },
    on_timer_stop: (app, timer) => {
      nodes[timer.address].timers = app.get_timers(timer.address);
    },
    on_timer_start: (app, timer) => {
      nodes[timer.address].timers = app.get_timers(timer.address);
    },
  });

  // Create the vue app.
  let vue_app = new Vue({
    el: '#clickthrough_app',
    data: {
      node: nodes[Echo.server.address],
    },
    methods: {
      trigger: function(timer) {
        clickthrough_app.run_timer(timer);
      },
      deliver: function(e) {
        clickthrough_app.deliver_message(e.message, e.index);
      },
      drop: function(e) {
        clickthrough_app.drop_message(e.message, e.index);
      },
      duplicate: function(e) {
        clickthrough_app.duplicate_message(e.message, e.index);
      },
    }
  });

  // Select a node by clicking it.
  for (let node of Object.values(nodes)) {
    node.svg.node.onclick = () => {
      vue_app.node = node;
    }
  }
}

function main() {
  simulated_app();
  // clickthrough_app();

  // let snap = Snap('#simulated_animation');
  // let Echo = zeno.examples.js.SimulatedEcho.Echo;

  // let vue_app = new Vue({
  //   el: '#app',
  //   data: {
  //     Echo: Echo,
  //     send_message: function(m, f) {
  //       console.log('sending message real quick.');
  //       f();
  //     }
  //   },
  // });
}

window.onload = main

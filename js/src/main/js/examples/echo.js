class VueNode {
  constructor(name, num_messages_received, log) {
    this.name = name;
    this.num_messages_received = num_messages_received || 0;
    this.log = log || [];
  }
}

function main() {
  let Echo = zeno.examples.js.Echo;
  let snap = Snap('#animation');

  let nodes = {};
  nodes[Echo.server.address] = {
    actor: Echo.server,
    svg: snap.circle(150, 50, 15),
    vue_node: new VueNode('Server'),
  }
  nodes[Echo.clientA.address] = {
    actor: Echo.clientA,
    svg: snap.circle(100, 150, 15),
    vue_node: new VueNode('Client A'),
  }
  nodes[Echo.clientB.address] = {
    actor: Echo.clientB,
    svg: snap.circle(200, 150, 15),
    vue_node: new VueNode('Client B'),
  }

  let vue_app = new Vue({
    el: '#app',
    data: { nodes: Object.values(nodes) },
  });

  let simulated_app = new zenojs.SimulatedApp(Echo.transport, {
    config_message: (app, message) => {
      let src = nodes[message.src];
      let dst = nodes[message.dst];
      let svg_message = snap.circle(src.svg.attr("cx"), src.svg.attr("cy"), 9)
                            .attr({fill: 'red'});
      snap.prepend(svg_message);
      return {
        svg_message: svg_message,
        animate_args: {cx: dst.svg.attr("cx"), cy: dst.svg.attr("cy")},
        timeout: 250 + Math.random() * 200,
        drop: Math.random() <= 0.1,
      };
    },
    on_send: (app, message) => {},
    on_deliver: (app, message) => {
      for (let node of Object.values(nodes)) {
        // Update num_messages_received.
        node.vue_node.num_messages_received = node.actor.numMessagesReceived;

        // Update log.
        for (let log_entry of node.actor.logger.bufferedLogsJs()) {
          // TODO(mwhittaker): Color logs.
          // TODO(mwhittaker): Append logs and autoscroll.
          node.vue_node.log.unshift(log_entry);
        }
        node.actor.logger.clearBufferedLogs();
      }
    },
    on_timer_stop: (app, timer) => {},
    on_timer_start: (app, timer) => {},
  });
}

window.onload = main

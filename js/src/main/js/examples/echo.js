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
    svg: snap.circle(150, 50, 20).attr(
        {fill: '#e74c3c', stroke: 'black', 'stroke-width': '3pt'}),
    vue_node: new VueNode('Server'),
  }
  nodes[Echo.clientA.address] = {
    actor: Echo.clientA,
    svg: snap.circle(75, 150, 20).attr(
        {fill: '#3498db', stroke: 'black', 'stroke-width': '3pt'}),
    vue_node: new VueNode('Client A'),
  }
  nodes[Echo.clientB.address] = {
    actor: Echo.clientB,
    svg: snap.circle(225, 150, 20).attr(
        {fill: '#2ecc71', stroke: 'black', 'stroke-width': '3pt'}),
    vue_node: new VueNode('Client B'),
  }

  // Add node titles.
  snap.text(150, 20, 'Server').attr({'text-anchor': 'middle'});
  snap.text(75, 190, 'Client A').attr({'text-anchor': 'middle'});
  snap.text(225, 190, 'Client B').attr({'text-anchor': 'middle'});

  let vue_app = new Vue({
    el: '#app',
    data: { vue_node: nodes[Echo.server.address].vue_node },
  });

  // Select a node by clicking it.
  for (let node of Object.values(nodes)) {
    node.svg.node.onclick = () => {
      vue_app.vue_node = node.vue_node;
    }
  }

  let simulated_app = new zenojs.SimulatedApp(Echo.transport, {
    config_message: (app, message) => {
      let src = nodes[message.src];
      let dst = nodes[message.dst];
      let svg_message = snap.circle(src.svg.attr("cx"), src.svg.attr("cy"), 9)
                            .attr({fill: '#2c3e50'});
      snap.prepend(svg_message);
      return {
        svg_message: svg_message,
        animate_args: {cx: dst.svg.attr("cx"), cy: dst.svg.attr("cy")},
        timeout: 250 + Math.random() * 200,
        drop: Math.random() <= 0.01,
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

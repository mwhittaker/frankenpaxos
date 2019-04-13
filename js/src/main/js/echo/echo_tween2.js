let client_info = {
  props: ['node'],

  data: function() {
    return {
      message: "",
    };
  },

  methods: {
    echo: function() {
      if (this.message === "") {
        return;
      }
      this.node.actor.echo(this.message);
      this.message = "";
    }
  },

  template: `
    <div>
      <div>{{node.actor.address.address}}</div>
      <div>Number of messages received: {{node.actor.numMessagesReceived}}</div>
      <button v-on:click="echo">Echo</button>
      <input v-model="message" v-on:keyup.enter="echo"></input>
    </div>
  `,
};

let server_info = {
  props: ['node'],

  template: `
    <div>
      <div>{{node.actor.address.address}}</div>
      <div>Number of messages received: {{node.actor.numMessagesReceived}}</div>
    </div>
  `,
};

function make_nodes(Echo, snap) {
  // Create the nodes.
  let nodes = {};
  nodes[Echo.server.address] = {
    actor: Echo.server,
    svg: snap.circle(150, 50, 20).attr(
        {fill: '#e74c3c', stroke: 'black', 'stroke-width': '3pt'}),
    component: server_info,
  };
  nodes[Echo.clientA.address] = {
    actor: Echo.clientA,
    svg: snap.circle(75, 150, 20).attr(
        {fill: '#3498db', stroke: 'black', 'stroke-width': '3pt'}),
    component: client_info,
  };
  nodes[Echo.clientB.address] = {
    actor: Echo.clientB,
    svg: snap.circle(225, 150, 20).attr(
        {fill: '#2ecc71', stroke: 'black', 'stroke-width': '3pt'}),
    component: client_info,
  };

  // Add node titles.
  snap.text(150, 20, 'Server').attr({'text-anchor': 'middle'});
  snap.text(75, 190, 'Client A').attr({'text-anchor': 'middle'});
  snap.text(225, 190, 'Client B').attr({'text-anchor': 'middle'});

  return nodes;
}

function make_app(Echo, snap, app_id) {
  let nodes = make_nodes(Echo, snap)

  let vue_app = new Vue({
    el: app_id,

    data: {
      node: nodes[Echo.server.address],
      transport: Echo.transport,
      time_scale: 1,
      auto_deliver_messages: true,
      auto_start_timers: true,
      send_message: (message) => {
        let src = nodes[message.src];
        let dst = nodes[message.dst];
        let svg_message =
          snap.circle(src.svg.attr("cx"), src.svg.attr("cy"), 9)
              .attr({fill: '#2c3e50'});
        snap.prepend(svg_message);
        return TweenMax.to(svg_message.node, 0.5, {
          attr:{
            cx: dst.svg.attr("cx"),
            cy: dst.svg.attr("cy"),
          },
          ease: Linear.easeNone,
          onComplete: function() {
            svg_message.remove();
          },
        });
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

function main() {
  make_app(frankenpaxos.echo.SimulatedEcho.Echo,
           Snap('#tweened_animation'),
           '#tweened_app');
}

window.onload = main

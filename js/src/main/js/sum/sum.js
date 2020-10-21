let client_info = {
  props: ['node'],

  data: function() {
    return {
      message: "",
    };
  },

  methods: {
    add: function() {
      if (this.message === "") {
        return;
      }
      this.node.actor.add(parseInt(this.message));
      this.message = "";
    }
  },

  template: `
    <div>
      <div>Number of messages received: {{node.actor.numMessagesReceived}}</div>
      <button v-on:click="add">Add</button>
      <input v-model="message" v-on:keyup.enter="add"></input>
    </div>
  `,
};

let server_info = {
  props: ['node'],

  template: `
    <div>
      <div>Running sum: {{node.actor.runningSum}}</div>
    </div>
  `,
};

function make_nodes(Sum, snap) {
  let colored = (color) => {
    return {
      fill: color,
      stroke: 'black',
      'stroke-width': '3pt',
    }
  };

  // Create the nodes.
  let nodes = {};
  nodes[Sum.server.address] = {
    actor: Sum.server,
    svgs: [
      snap.circle(150, 50, 20).attr(colored('#e74c3c')),
    ],
    color: '#e74c3c',
    component: server_info,
  };
  nodes[Sum.clientA.address] = {
    actor: Sum.clientA,
    svgs: [
      snap.circle(75, 150, 20).attr(colored('#3498db')),
    ],
    color: '#3498db',
    component: client_info,
  };
  nodes[Sum.clientB.address] = {
    actor: Sum.clientB,
    svgs: [
      snap.circle(225, 150, 20).attr(colored('#2ecc71')),
    ],
    color: '#2ecc71',
    component: client_info,
  };

  // Add node titles.
  snap.text(150, 20, 'Server').attr({'text-anchor': 'middle'});
  snap.text(75, 190, 'Client A').attr({'text-anchor': 'middle'});
  snap.text(225, 190, 'Client B').attr({'text-anchor': 'middle'});

  return nodes;
}

function main() {
  let Sum = frankenpaxos.sum.Sum.Sum;
  let snap = Snap('#animation');
  let nodes = make_nodes(Sum, snap)

  let vue_app = new Vue({
    el: '#app',

    data: {
      nodes: nodes,
      node: nodes[Sum.server.address],
      transport: Sum.transport,
      settings: {
        time_scale: 1,
        auto_deliver_messages: true,
        auto_start_timers: true,
      },
    },

    methods: {
      send_message: function(message) {
        let src = this.nodes[message.src];
        let dst = this.nodes[message.dst];
        let src_x = src.svgs[0].attr("cx");
        let src_y = src.svgs[0].attr("cy");
        let dst_x = dst.svgs[0].attr("cx");
        let dst_y = dst.svgs[0].attr("cy");

        let svg_message = snap.circle(src_x, src_y, 9).attr({fill: '#2c3e50'});
        snap.prepend(svg_message);
        return TweenMax.to(svg_message.node, 0.5, {
          attr: { cx: dst_x, cy: dst_y },
          ease: Linear.easeNone,
          onComplete: () => { svg_message.remove(); },
        });
      },

      partition: function(address) {
        this.nodes[address].svgs[0].attr({fill: "#7f8c8d"});
      },

      unpartition: function(address) {
        this.nodes[address].svgs[0].attr({fill: this.nodes[address].color});
      },
    },
  });

  // Select a node by clicking it.
  for (let node of Object.values(nodes)) {
    for (let svg of node.svgs) {
      svg.node.onclick = () => {
        vue_app.node = node;
      }
    }
  }
}

window.onload = main

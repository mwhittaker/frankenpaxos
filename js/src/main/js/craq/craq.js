// Node components /////////////////////////////////////////////////////////////
const client_info = {
  props: {
    node: Object,
  },

  data: function() {
    return {
      write_key: "",
      write_value: "",
      read_key: "",
    };
  },

  methods: {
    read: function() {
      if (this.read_key === "") {
        return;
      }
      this.node.actor.read(0, this.read_key);
      this.read_key = "";
    },

    read_ten: function() {
      if (this.read_key === "") {
        return;
      }
      for (let i = 0; i < 10; ++i) {
        this.node.actor.read(i, this.read_key);
      }
      this.read_key = "";
    },

    write: function() {
      if (this.write_key === "") {
        return;
      }
      this.node.actor.write(0, this.write_key, this.write_value);
      this.write_key = "";
      this.write_value = "";
    },

    write_ten: function() {
      if (this.write_key === "") {
        return;
      }
      for (let i = 0; i < 10; ++i) {
        this.node.actor.write(i, this.write_key, this.write_value);
      }
      this.write_key = "";
      this.write_value = "";
    },
  },

  template: `
    <div>
      <div>
        ids = <frankenpaxos-map :map="node.actor.ids"></frankenpaxos-map>
      </div>

      <div>
        growingBatch =
        <frankenpaxos-seq :seq="node.actor.growingBatch">
        </frankenpaxos-seq>
      </div>

      <div>
        growingReadBatch =
        <frankenpaxos-seq :seq="node.actor.growingReadBatch">
        </frankenpaxos-seq>
      </div>

      <div>
        states =
        <frankenpaxos-map :map="node.actor.states" v-slot="{value: state}">
          <div v-if="state.constructor.name.includes('PendingWrite')">
            PendingWrite
            <fp-object>
              <fp-field :name="'id'">{{state.id}}</fp-field>
              <fp-field :name="'command'">{{state.command}}</fp-field>
              <fp-field :name="'result'">{{state.result}}</fp-field>
              <fp-field :name="'resendClientRequest'">
                {{state.resendClientRequest}}
              </fp-field>
            </fp-object>
          </div>


          <div v-if="state.constructor.name.includes('PendingRead')">
            PendingRead
            <fp-object>
              <fp-field :name="'id'">{{state.id}}</fp-field>
              <fp-field :name="'command'">{{state.command}}</fp-field>
              <fp-field :name="'result'">{{state.result}}</fp-field>
              <fp-field :name="'resendReadRequest'">
                {{state.resendReadRequest}}
              </fp-field>
            </fp-object>
          </div>
        </frankenpaxos-map>
      </div>

      <div>
        <button v-on:click="read">Read</button>
        <button v-on:click="read_ten">Read Ten</button>
        <input v-model="read_key"></input>
      </div>
      <div>
        <button v-on:click="write">Write</button>
        <button v-on:click="write_ten">Write Ten</button>
        <input v-model="write_key"></input>
        <input v-model="write_value"></input>
      </div>
    </div>
  `,
}

const chain_node_info = {
  props: {
    node: Object,
  },

  template: `
    <div>
      <div>
        index = {{node.actor.index}}
      </div>

      <div>
        nextIndex = {{node.actor.nextIndex}}
      </div>

      <div>
        prevIndex = {{node.actor.prevIndex}}
      </div>

      <div>
        isHead = {{node.actor.isHead}}
      </div>

      <div>
        isTail = {{node.actor.isTail}}
      </div>

      <div>
        versions = {{node.actor.versions}}
      </div>

      <div>
        pendingWrites =
        <frankenpaxos-seq :seq="node.actor.pendingWrites">
        </frankenpaxos-seq>
      </div>

      <div>
        stateMachine =
        <frankenpaxos-map :map="node.actor.stateMachine">
        </frankenpaxos-map>
      </div>
    </div>
  `,
}

// Main app ////////////////////////////////////////////////////////////////////
function distance(x1, y1, x2, y2) {
  const dx = x1 - x2;
  const dy = y1 - y2;
  return Math.sqrt(dx*dx + dy*dy);
}

function make_nodes(Craq, snap, batched) {
  // https://flatuicolors.com/palette/defo
  const flat_red = '#e74c3c';
  const flat_dark_blue = '#2c3e50';

  const colored = (color) => {
    return {
      'fill': color,
      'stroke': 'black', 'stroke-width': '3pt',
    }
  };

  const number_style = {
    'text-anchor': 'middle',
    'alignment-baseline': 'middle',
    'font-size': '20pt',
    'font-weight': 'bolder',
    'fill': 'black',
    'stroke': 'white',
    'stroke-width': '1px',
  }

  const client_x = 100;
  const chain_node_x = 400;

  const nodes = {};

  // Clients.
  const clients = [
    {client: Craq.client1, y: 100},
    {client: Craq.client2, y: 200},
    {client: Craq.client3, y: 300},
    {client: Craq.client4, y: 400},
  ]
  for (const [index, {client, y}] of clients.entries()) {
    const color = flat_red;
    nodes[client.address] = {
      actor: client,
      color: color,
      component: client_info,
      svgs: [
        snap.circle(client_x, y, 20).attr(colored(color)),
        snap.text(client_x, y, (index + 1).toString()).attr(number_style),
      ],
    };
  }

  // Chain Nodes.
  const chainNodes = [
    {chainNode: Craq.chainNode1, y: 150, name: 'H'},
    {chainNode: Craq.chainNode2, y: 250, name: 'M'},
    {chainNode: Craq.chainNode3, y: 350, name: 'T'},
  ]
  for (const [index, {chainNode, y, name}] of chainNodes.entries()) {
    const color = flat_dark_blue;
    nodes[chainNode.address] = {
      actor: chainNode,
      color: color,
      component: chain_node_info,
      svgs: [
        snap.circle(chain_node_x, y, 20).attr(colored(color)),
        snap.text(chain_node_x, y, name).attr(number_style),
      ],
    };
  }

  // Node titles.
  const anchor_middle = (text) => text.attr({'text-anchor': 'middle'});
  anchor_middle(snap.text(client_x, 50, 'Clients'));
  anchor_middle(snap.text(chain_node_x, 50, 'Chain Nodes'));

  return nodes;
}

function unbatched() {
  const Craq = frankenpaxos.craq.Craq.Craq;
  const snap = Snap('#animation');
  const nodes = make_nodes(Craq, snap, batch=false);

  // Create the vue app.
  let vue_app = new Vue({
    el: '#app',

    data: {
      nodes: nodes,
      node: nodes[Craq.client1.address],
      transport: Craq.transport,
      settings: {
        time_scale: 1,
        auto_deliver_messages: true,
        auto_start_timers: true,
      },
    },

    methods: {
      send_message: function(message) {
        let src = nodes[message.src];
        let dst = nodes[message.dst];
        let src_x = src.svgs[0].attr("cx");
        let src_y = src.svgs[0].attr("cy");
        let dst_x = dst.svgs[0].attr("cx");
        let dst_y = dst.svgs[0].attr("cy");
        let d = distance(src_x, src_y, dst_x, dst_y);
        let speed = 400; // px per second.

        let svg_message = snap.circle(src_x, src_y, 9).attr({fill: '#2c3e50'});
        snap.prepend(svg_message);
        let duration = d / speed;
        return TweenMax.to(svg_message.node, duration, {
          attr: { cx: dst_x, cy: dst_y },
          ease: Linear.easeNone,
          onComplete: () => { svg_message.remove(); },
        });
      },

      partition: function(address) {
        nodes[address].svgs[0].attr({fill: "#7f8c8d"})
      },

      unpartition: function(address) {
        nodes[address].svgs[0].attr({fill: nodes[address].color})
      },
    },
  });

  // Select a node by clicking it.
  for (const node of Object.values(nodes)) {
    for (const svg of node.svgs) {
      svg.node.onclick = () => {
        vue_app.node = node;
      }
    }
  }
}

function batched() {
  const Craq = frankenpaxos.craq.BatchedCraq.Craq;
  const snap = Snap('#batched_animation');
  const nodes = make_nodes(Craq, snap, batch=true);

  // Create the vue app.
  let vue_app = new Vue({
    el: '#batched_app',

    data: {
      nodes: nodes,
      node: nodes[Craq.client1.address],
      transport: Craq.transport,
      settings: {
        time_scale: 1,
        auto_deliver_messages: true,
        auto_start_timers: true,
      },
    },

    methods: {
      send_message: function(message) {
        let src = nodes[message.src];
        let dst = nodes[message.dst];
        let src_x = src.svgs[0].attr("cx");
        let src_y = src.svgs[0].attr("cy");
        let dst_x = dst.svgs[0].attr("cx");
        let dst_y = dst.svgs[0].attr("cy");
        let d = distance(src_x, src_y, dst_x, dst_y);
        let speed = 400 + (Math.random() * 50); // px per second.

        let svg_message = snap.circle(src_x, src_y, 9).attr({fill: '#2c3e50'});
        snap.prepend(svg_message);
        let duration = d / speed;
        return TweenMax.to(svg_message.node, duration, {
          attr: { cx: dst_x, cy: dst_y },
          ease: Linear.easeNone,
          onComplete: () => { svg_message.remove(); },
        });
      },

      partition: function(address) {
        nodes[address].svgs[0].attr({fill: "#7f8c8d"})
      },

      unpartition: function(address) {
        nodes[address].svgs[0].attr({fill: nodes[address].color})
      },
    },
  });

  // Select a node by clicking it.
  for (const node of Object.values(nodes)) {
    for (const svg of node.svgs) {
      svg.node.onclick = () => {
        vue_app.node = node;
      }
    }
  }
}

function main() {
  unbatched();
  batched();
}

window.onload = main

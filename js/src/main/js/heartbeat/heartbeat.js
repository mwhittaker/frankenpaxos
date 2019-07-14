let participant_info = {
  props: ['node'],

  template: `
    <div>
      <div>
        addresses =
        <frankenpaxos-seq :seq="node.actor.addresses"></frankenpaxos-seq>
      </div>

      <div>
        numRetries =
        <frankenpaxos-seq :seq="node.actor.numRetries"></frankenpaxos-seq>
      </div>

      <div>
        networkDelayNanos =
        <frankenpaxos-map :map="node.actor.networkDelayNanos">
        </frankenpaxos-map>
      </div>

      <div>
        alive =
        <frankenpaxos-set :set="node.actor.alive"></frankenpaxos-set>
      </div>
    </div>
  `,
};

function make_nodes(Heartbeat, snap) {
  // https://flatuicolors.com/palette/defo
  let colored = (color) => {
    return {
      'fill': color,
      'stroke': 'black',
      'stroke-width': '3pt',
    }
  };

  let polar_to_cartesian = function(x_origin, y_origin, theta, r) {
    let dx = Math.cos(theta) * r;
    let dy = Math.sin(theta) * r;
    return [x_origin + dx, y_origin - dy];
  }

  // Nodes.
  let nodes = {};
  let title_attr = {'text-anchor': 'middle', 'alignment-baseline': 'middle'};
  let x_origin = 200;
  let y_origin = 150;
  let theta = 2 * Math.PI / 5;
  let r = 100;
  let flat_red = '#e74c3c';

  let [ax, ay] = polar_to_cartesian(x_origin, y_origin, 1 * theta, r);
  let [bx, by] = polar_to_cartesian(x_origin, y_origin, 2 * theta, r);
  let [cx, cy] = polar_to_cartesian(x_origin, y_origin, 3 * theta, r);
  let [dx, dy] = polar_to_cartesian(x_origin, y_origin, 4 * theta, r);
  let [ex, ey] = polar_to_cartesian(x_origin, y_origin, 5 * theta, r);

  nodes[Heartbeat.a.address] = {
    actor: Heartbeat.a,
    color: flat_red,
    component: participant_info,
    svgs: [
      snap.circle(ax, ay, 20).attr(colored(flat_red)),
      snap.text(ax, ay, 'a').attr(title_attr),
    ],
  };
  nodes[Heartbeat.b.address] = {
    actor: Heartbeat.b,
    color: flat_red,
    component: participant_info,
    svgs: [
      snap.circle(bx, by, 20).attr(colored(flat_red)),
      snap.text(bx, by, 'b').attr(title_attr),
    ],
  };
  nodes[Heartbeat.c.address] = {
    actor: Heartbeat.c,
    color: flat_red,
    component: participant_info,
    svgs: [
      snap.circle(cx, cy, 20).attr(colored(flat_red)),
      snap.text(cx, cy, 'c').attr(title_attr),
    ],
  };
  nodes[Heartbeat.d.address] = {
    actor: Heartbeat.d,
    color: flat_red,
    component: participant_info,
    svgs: [
      snap.circle(dx, dy, 20).attr(colored(flat_red)),
      snap.text(dx, dy, 'd').attr(title_attr),
    ],
  };
  nodes[Heartbeat.e.address] = {
    actor: Heartbeat.e,
    color: flat_red,
    component: participant_info,
    svgs: [
      snap.circle(ex, ey, 20).attr(colored(flat_red)),
      snap.text(ex, ey, 'e').attr(title_attr),
    ],
  };

  return nodes
}

function main() {
  let Heartbeat = frankenpaxos.heartbeat.TweenedHeartbeat.Heartbeat;
  let snap = Snap('#animation');
  let nodes = make_nodes(Heartbeat, snap);
  let serializer = nodes[Heartbeat.a.address].actor.serializer;

  // Create the vue app.
  let vue_app = new Vue({
    el: '#app',

    data: {
      nodes: nodes,
      node: nodes[Heartbeat.a.address],
      transport: Heartbeat.transport,
      settings: {
        time_scale: 1,
        auto_deliver_messages: true,
        auto_start_timers: true,
      },
    },

    methods: {
      distance: function(x1, y1, x2, y2) {
        const dx = x1 - x2;
        const dy = y1 - y2;
        return Math.sqrt(dx*dx + dy*dy);
      },

      send_message: function(message) {
        let from_bytes = serializer.fromBytes(message.bytes);
        let string_message = serializer.toPrettyString(from_bytes);
        let color = string_message.includes("ping") ? '#3498db' : '#2ecc71';
        let src = nodes[message.src];
        let dst = nodes[message.dst];
        let src_x = src.svgs[0].attr("cx");
        let src_y = src.svgs[0].attr("cy");
        let dst_x = dst.svgs[0].attr("cx");
        let dst_y = dst.svgs[0].attr("cy");
        let d = this.distance(src_x, src_y, dst_x, dst_y);
        let speed = 100 + (Math.random() * 50); // px per second.

        let svg_message = snap.circle(src_x, src_y, 9).attr({fill: color});
        snap.prepend(svg_message);
        let duration = d / speed;
        return TweenMax.to(svg_message.node, duration, {
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

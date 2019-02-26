// Returns numbers drawn from an exponential distribution with mean `mean`.
let exponential = function(mean) {
  return -Math.log(Math.random() + Number.EPSILON) * mean
}

function make_nodes(Heartbeat, snap) {
  // https://flatuicolors.com/palette/defo
  let colored = (color) => {
    return {
      'fill': color,
      'stroke': 'black', 'stroke-width': '3pt',
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
    svgs: [
      snap.circle(ax, ay, 20).attr(colored(flat_red)),
      snap.text(ax, ay, 'a').attr(title_attr),
    ],
  };
  nodes[Heartbeat.b.address] = {
    actor: Heartbeat.b,
    svgs: [
      snap.circle(bx, by, 20).attr(colored(flat_red)),
      snap.text(bx, by, 'b').attr(title_attr),
    ],
  };
  nodes[Heartbeat.c.address] = {
    actor: Heartbeat.c,
    svgs: [
      snap.circle(cx, cy, 20).attr(colored(flat_red)),
      snap.text(cx, cy, 'c').attr(title_attr),
    ],
  };
  nodes[Heartbeat.d.address] = {
    actor: Heartbeat.d,
    svgs: [
      snap.circle(dx, dy, 20).attr(colored(flat_red)),
      snap.text(dx, dy, 'd').attr(title_attr),
    ],
  };
  nodes[Heartbeat.e.address] = {
    actor: Heartbeat.e,
    svgs: [
      snap.circle(ex, ey, 20).attr(colored(flat_red)),
      snap.text(ex, ey, 'e').attr(title_attr),
    ],
  };

  return nodes
}

function make_app(Heartbeat, snap, app_id) {
  let nodes = make_nodes(Heartbeat, snap);
  let serializer = nodes[Heartbeat.a.address].actor.serializer;

  // Create the vue app.
  let vue_app = new Vue({
    el: app_id,

    data: {
      transport: Heartbeat.transport,
      node: nodes[Heartbeat.a.address],
      send_message: (message, callback) => {
        let from_bytes = serializer.fromBytes(message.bytes);
        let string_message = serializer.toPrettyString(from_bytes);
        let color = string_message.includes("ping") ? '#3498db' : '#2ecc71';
        let src = nodes[message.src];
        let dst = nodes[message.dst];
        let svg_message =
          snap.circle(src.svgs[0].attr("cx"), src.svgs[0].attr("cy"), 7)
              .attr({fill: color});
        snap.prepend(svg_message);
        svg_message.animate(
          {cx: dst.svgs[0].attr("cx"), cy: dst.svgs[0].attr("cy")},
          300 + exponential(200),
          callback);
      }
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

function main() {
  make_app(frankenpaxos.heartbeat.SimulatedHeartbeat.Heartbeat,
           Snap('#simulated_animation'),
           '#simulated_app');

  make_app(frankenpaxos.heartbeat.ClickthroughHeartbeat.Heartbeat,
           Snap('#clickthrough_animation'),
           '#clickthrough_app');
}

window.onload = main

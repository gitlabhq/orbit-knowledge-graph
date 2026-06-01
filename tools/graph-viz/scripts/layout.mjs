// Offline 3D force layout (Milestone-2 performance path).
//
// Runs d3-force-3d in Node so the renderer can ship precomputed `x,y,z` and
// freeze the live simulation (`cooldownTicks(0)`). At ~24k nodes the live
// simulation is the dominant CPU cost; precomputing it once here makes the
// first frame static and cheap. Mutates each node in place with x/y/z and
// returns the node array.

import {
  forceSimulation,
  forceManyBody,
  forceLink,
  forceCenter,
} from 'd3-force-3d';

/**
 * @param {Array<{id:string,degree?:number}>} nodes
 * @param {Array<{source:string,target:string}>} links
 * @param {{ticks?:number, chargeStrength?:number, linkDistance?:number}} [opts]
 */
export function layoutGraph(nodes, links, opts = {}) {
  const ticks = opts.ticks ?? 300;
  const chargeStrength = opts.chargeStrength ?? -120;
  const linkDistance = opts.linkDistance ?? 40;

  // d3-force mutates the node objects it is given; clone link endpoints so the
  // shipped graph keeps plain string ids rather than resolved node references.
  const simLinks = links.map((l) => ({ source: l.source, target: l.target }));

  const sim = forceSimulation(nodes, 3)
    .force('charge', forceManyBody().strength(chargeStrength))
    .force(
      'link',
      forceLink(simLinks)
        .id((n) => n.id)
        .distance(linkDistance),
    )
    .force('center', forceCenter())
    .stop();

  for (let i = 0; i < ticks; i += 1) sim.tick();

  for (const n of nodes) {
    n.x = round(n.x);
    n.y = round(n.y);
    n.z = round(n.z);
    // d3 leaves velocity fields on the nodes; drop them from the payload.
    delete n.vx;
    delete n.vy;
    delete n.vz;
    delete n.index;
  }
  return nodes;
}

function round(v) {
  return Math.round((v ?? 0) * 100) / 100;
}

# Power-flow diagram mockup — recurring-mistakes checklist

Check this list on every iteration of `live-power-flow-dashboard-mockup-v5.html`
(or its successors) before calling a change done. Each item below was a real
regression caught in review at least once already.

## Arrow bleeding
A colored stroke must never extend past its arrowhead. `stroke-linecap:
round` makes a `<line>`'s visible extent bigger than its literal
coordinates, so if a shaft line is drawn all the way to the same tip point
the arrowhead polygon is drawn at, the rounded cap pokes out past the
triangle's edges. Always shorten shaft segments by the arrowhead's `size`
before drawing them, and draw the arrowhead as a separate polygon at the
true, un-shortened tip. This applies per-stripe too when an edge is
striped - shorten every stripe's endpoint by the same amount, not just a
single aggregate line.

## Ugly thickness quantization
After any change to the watts->thickness formula or to
MIN/MAX_THICKNESS_PX, replot it on the comparison chart
(`renderThicknessChart`) and look for buckets whose watt-range is wildly
uneven vs. its neighbors (rule of thumb: no bucket more than ~1.5x wider
than the one next to it), especially near zero. An additive `MIN +
ratio*(MAX-MIN)` formula silently halves the first real bucket's width
relative to the others, because the "0 watts" state absorbs part of what
should be the first bucket's range. Use `Math.max(MIN, ratio*MAX)`
instead - a plain floor clamp on a curve that already passes through 0
- not an additive offset.

## Arrow overlap / crossing
Two edges' visible paths (including bent-path elbows) must never occupy
the same pixels unless they share a real node (a legitimate convergence
point - Junction's dot, or two stripes on the same composite arrow).
When adding a new bent path, check every row/column it crosses for a box
that stretches to fill its row (Battery and the wide nodes do; Inverter/
Junction, being small, don't) - the elbow has to clear the tallest thing
in the row, not just the node the path started from.

## Arrows must touch their box's actual border
Every edge's endpoint must come from `edgeX`/`edgeY`/`centerX`/`centerY`
against the real element, never a guessed/hardcoded offset - box sizes
change (square inverter, details expanded, etc.) and a hardcoded offset
silently goes stale.

## Source-color consistency
A source's stripe color in *any* downstream arrow must match that
source's own box border/icon color. If a box's status-color mapping
changes (e.g. battery discharging switching from orange to yellow so it
doesn't collide with grid-import's orange in a shared stripe set), check
every edge that stripes or single-colors based on that source and update
it too - both the box and every arrow fed by it need to agree, including
transitively downstream (source -> hub -> further sink). This applies to
*destination* boxes too, not just sources: an edge's own color/thickness
often gets computed with its own separate expression (e.g. "orange if
active else grey") rather than reading the destination node's own
already-computed color - if the node's own coloring later grows another
case (e.g. Backup's overload turning the node red), that separate
expression silently drifts out of sync unless it's refactored into one
shared function both the node and its arrow(s) call.

## Manhattan bends need a real elbow anchor, not just shared coordinates
A "Junction"-style node that's drawn as a small visible dot inside a
larger invisible drag hit-box (so it's easy to grab on a phone) needs its
own edge-anchor logic - the generic edgeX/edgeY-against-the-real-element
helpers will anchor to the *hit-box's* border, not the dot's, leaving a
visible gap between the arrow tip and the dot once the hit-box grows past
the dot's own size.

## A status/alert color is not a flow color
A box's own status color can include alert states (e.g. "red" for an
overload or a reserve-floor-hit) that have no equivalent meaning as a
flow color - nothing else in the diagram uses red to mean "this is what's
flowing," only yellow/green/orange do. If a source's arrow is drawn using
that same status-color value directly, hitting the alert state makes the
arrow flip to a color that doesn't correspond to any real flow type,
which reads as nonsense ("an alert is flowing"). Keep a separate
`flowColor` (never includes alert colors) for anything that draws an
arrow/stripe, and reserve the status color (which can) for the node
itself.

## Stripe order must follow the direction of travel, not the raw axis
Don't assign a stripe's perpendicular offset using a fixed rule like
"vertical segments order left-to-right, horizontal segments order
top-to-bottom" - that rule is blind to which way the segment is actually
travelling, so the same stripe ends up on opposite physical sides
depending on whether the segment happens to run left-to-right or
right-to-left (e.g. after a box gets dragged to the other side of its
neighbor). Compute the offset from a perpendicular vector derived from
the segment's own direction of travel (a consistent 90-degree rotation,
`{x:-dy, y:dx}` - "always the right-hand side of travel") instead, so a
given stripe index stays on the same side *relative to the flow*
regardless of which way that flow points on screen. When picking the
rotation's sign, check it against a real straight (unbent) edge whose
correct left-to-right/top-to-bottom order is known from where the source
boxes actually sit on screen - it's easy to get the rotation direction
itself right (self-consistent through any bend either way) while still
picking the wrong chirality (mirrored) for a plain straight segment.

## Manhattan-only
Every segment purely horizontal or vertical (`x1===x2` or `y1===y2`).
No diagonals, ever, including inside bent paths and arrowhead
construction.

## Striped edges must stripe every segment, not just the final one
A bent (Manhattan "L"/"Z") path is drawn as multiple segments, but only
the last one touches the destination box and carries the arrowhead. It's
tempting to draw the earlier segments as a single plain color and only
stripe the final segment - looks fine at a glance but reads as "this flow
only just became mixed right before the sink," which is wrong; the mix
is real for the whole path.

## A striped bent path is one continuous shape per stripe, not stitched segments
Three approaches were tried, in order, for striping a Manhattan path that
bends (more than one segment) - the first two both left a real defect
right at the bend:
1. **Independent butt-capped lines per segment**, meeting at the shared
   joint coordinate. Each segment's stripes are offset perpendicular to
   *its own* direction of travel, so two segments meeting at a corner
   offset along *different* axes - their bands don't actually overlap at
   the joint, leaving a real gap, even though it's invisible for a
   round-capped single-color line (whose cap bulges over the same gap).
2. **Extending each segment past the joint** (an overshoot, or a small
   patch centered on the corner) to paper over that gap. Both still leave
   the joint built from two *independent* shapes, which - even with
   correct math - visibly cross each other at a slight angle right at the
   bend, since an overshoot is still a straight line continuing the
   *original* segment's direction, not curving to meet the next one.

Fixed by not stitching segments together at all: build **one polygon per
stripe that traces the entire bent centerline** (every segment in the
path), offset by that stripe's own perpendicular distance on each side,
with an *exact* miter at every interior corner rather than an
approximation. For a 90-degree Manhattan turn this miter point has a
closed form - `corner + perpIncoming*offset + perpOutgoing*offset` -
because perpendicular-to-perpendicular segments always rotate to exactly
one x-only and one y-only perpendicular vector, so summing them can't
double-count either axis. One exact shape per stripe means there is no
joint left to get wrong.

## Striped arrowhead: one triangle, filled by a hard-stop gradient
Two earlier approaches to the tip:
1. **A wedge fan** - one triangle per stripe, `[backPoint, tip,
   backPoint]`, sharing the tip vertex - produced visible seams/slivers
   where all the separate triangles converge on the exact same pixel.
2. **A single real triangle as a `<clipPath>`**, filled by per-stripe
   rectangles clipped to its silhouette - fixed the tip-vertex seam (only
   one triangle outline exists), but was still a stack of several
   separate shapes, and didn't address the *shaft*'s own joint problems
   at all.

Replaced by filling the *one* real triangle polygon with a single
`<linearGradient gradientUnits="userSpaceOnUse">` that has duplicate
(hard) stops at each stripe boundary instead of a gradual blend. Point
the gradient's own axis along `perpVector(dir)` (perpendicular to
travel), spanning the triangle's back-edge half-width - color then stays
constant along the direction of travel and only varies across it, and
the triangle's own taper (not a separate clip shape) is what narrows the
bands toward the tip. There's structurally nothing left to seam: the
whole tip is exactly one shape with exactly one fill.

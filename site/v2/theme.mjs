export const COLORS=["#63A2D9","#7FBF85","#E2D68B","#B493BA","#E2AC8B","#6D73B1","#D38C9E","#977B72","#6BB3AE","#B16D98","#B1705C","#7B8E4B","#84848F","#A9CCEA","#AEDEB2","#EAE4BF","#D8C6DC","#E6C3AD","#AFB3D9","#DCB1BC","#CFB7B0","#B2DCD9","#D1A9C2","#D1A294","#C6D2A7","#C8C8CA"];
export const TEXT_COLOR="#25282D";
export const FONT="TWK Lausanne Pan 250";
// Paired comparisons share hue; main/light distinguish related subseries.
export function seriesColor(index,paired=false){return COLORS[paired?(Math.floor(index/2)%13+(index%2?13:0)):index%13];}

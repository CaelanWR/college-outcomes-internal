// Keep a double-click from opening the single-click report first.
export function bindPlotClick(target,{single,double,delay=400}={}){
 let pending;
 const cancel=()=>{clearTimeout(pending);pending=undefined;};
 target.addEventListener('click',event=>{cancel();if(event.detail>=2)return;if(!double){single?.();return;}pending=setTimeout(()=>{pending=undefined;if(target.isConnected!==false)single?.();},delay);});
 target.addEventListener('dblclick',event=>{event.preventDefault();event.stopPropagation();cancel();double?.();});
 target.addEventListener('keydown',event=>{if(event.key==='Enter'||event.key===' '){event.preventDefault();cancel();if(event.shiftKey&&double)double();else (single||double)?.();}});
 return cancel;
}
export function createIsolation(size,onChange){
 let visible=Array(size).fill(true);
 const update=()=>onChange([...visible]);
 return {get visible(){return [...visible];},toggle(index){visible[index]=!visible[index];update();},isolate(index){const alone=visible.every((shown,i)=>shown===(i===index));visible=visible.map((_,i)=>alone||i===index);update();},reset(){visible.fill(true);update();}};
}

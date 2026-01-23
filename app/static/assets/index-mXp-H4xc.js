import{c,r as t}from"./index-DtQn1uNd.js";/**
 * @license lucide-react v0.462.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const s=c("Check",[["path",{d:"M20 6 9 17l-5-5",key:"1gmf2c"}]]);function i(r){const e=t.useRef({value:r,previous:r});return t.useMemo(()=>(e.current.value!==r&&(e.current.previous=e.current.value,e.current.value=r),e.current.previous),[r])}var o=t.createContext(void 0);function a(r){const e=t.useContext(o);return r||e||"ltr"}function f(r,[e,n]){return Math.min(n,Math.max(e,r))}export{s as C,a,f as c,i as u};

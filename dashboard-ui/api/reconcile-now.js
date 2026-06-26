import { proxy } from "./_proxy.js";
export default (req, res) => proxy(req, res, "/reconcile-now", ["POST"]);

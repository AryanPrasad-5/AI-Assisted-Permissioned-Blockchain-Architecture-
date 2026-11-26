import { Link } from "react-router-dom";
export default function NotFound() {
  return (
    <div className="page">
      <div className="state">
        <strong>There is no page at this address.</strong>
        <span>The link may be from an earlier version of the site.</span>
        <Link className="btn btn-quiet btn-sm" to="/research">Go to the research overview</Link>
      </div>
    </div>
  );
}

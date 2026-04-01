export default function InsightCard({ title, value, valueColor }) {
  return (
    <div className="insight-card" style={{ "--metric-accent": valueColor || "#17312c" }}>
      <div className="insight-card-label">{title}</div>
      <div className="insight-card-value" style={{ color: valueColor || undefined }}>
        {value || "-"}
      </div>
    </div>
  );
}

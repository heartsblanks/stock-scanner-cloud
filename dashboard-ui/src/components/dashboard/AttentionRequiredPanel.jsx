function severityClass(severity) {
  if (severity === "critical") {
    return "dashboard-attention-item dashboard-attention-item-critical";
  }
  if (severity === "warning") {
    return "dashboard-attention-item dashboard-attention-item-warning";
  }
  return "dashboard-attention-item dashboard-attention-item-info";
}

export default function AttentionRequiredPanel({ items = [] }) {
  if (!items.length) {
    return null;
  }

  return (
    <section className="dashboard-section">
      <div className="dashboard-panel dashboard-panel-strong">
        <div className="dashboard-panel-body">
          <div className="dashboard-panel-heading">
            <div>
              <h2 className="dashboard-panel-title">Attention Required</h2>
            </div>
          </div>
          <div className="dashboard-attention-list">
            {items.map((item) => (
              <div key={item.id} className={severityClass(item.severity)}>
                <div className="dashboard-attention-title">{item.title}</div>
                <div className="dashboard-attention-detail">{item.detail}</div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </section>
  );
}

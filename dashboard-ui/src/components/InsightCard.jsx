export default function InsightCard({ title, value, valueColor, onClick, interactive = false }) {
  const interactiveProps = interactive
    ? {
        role: "button",
        tabIndex: 0,
        onClick,
        onKeyDown: (event) => {
          if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            onClick?.();
          }
        },
      }
    : {};

  return (
    <div
      className={`insight-card${interactive ? " insight-card-interactive" : ""}`}
      style={{ "--metric-accent": valueColor || "#17312c" }}
      {...interactiveProps}
    >
      <div className="insight-card-label">{title}</div>
      <div className="insight-card-value" style={{ color: valueColor || undefined }}>
        {value || "-"}
      </div>
    </div>
  );
}

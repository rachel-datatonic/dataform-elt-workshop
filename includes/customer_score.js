function customer_score_expr() {
  // A SQL formula for scoring; tweak weights as needed.
  return `
    0.5 * LEAST(lifetime_spend / 1000, 1) +
    0.3 * LEAST(order_count / 10, 1) +
    0.2 * (CASE WHEN days_since_last_order < 90 THEN 1 ELSE 0 END)
  `;
}

module.exports = { customer_score_expr };

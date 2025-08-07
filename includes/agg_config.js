const aggConfig = [
    { column: "order_amount", aggs: ["SUM", "AVG", "MAX", "MEDIAN"] },
    { column: "order_count", aggs: ["SUM", "AVG"] }
  ];
  
  // Generates a SQL SELECT fragment for aggregation
  function build_agg_selects() {
    let selectClauses = [];
    aggConfig.forEach(cfg => {
      cfg.aggs.forEach(agg => {
        if (agg === "MEDIAN") {
          selectClauses.push(`APPROX_QUANTILES(${cfg.column}, 2)[OFFSET(1)] AS ${cfg.column}_median`);
        } else {
          selectClauses.push(`${agg}(${cfg.column}) AS ${cfg.column}_${agg.toLowerCase()}`);
        }
      });
    });
    return selectClauses.join(",\n  ");
  }
  
  module.exports = { build_agg_selects };
  
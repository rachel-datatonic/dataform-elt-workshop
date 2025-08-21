const segments = {
    "High Value": "lifetime_spend > 1000 AND order_count >= 5",
    "Frequent Shopper": "order_count > 5 AND lifetime_spend <= 1000",
    "New Customer": "order_count = 1",
    "At Risk": "days_since_last_order > 90 AND days_since_last_order < 365",
    "Churned": "days_since_last_order >= 365",
    "VIP": "lifetime_spend > 2000 OR order_count > 20"
};

// Dynamic CASE builder for segmentation
function build_segment_case_statement() {
    let caseStatement = "CASE\n";
    for (const segmentName in segments) {
        caseStatement += `    WHEN ${segments[segmentName]} THEN '${segmentName}'\n`;
    }
    caseStatement += "    ELSE 'General'\n  END";
    return caseStatement;
}

module.exports = {
    build_segment_case_statement
};

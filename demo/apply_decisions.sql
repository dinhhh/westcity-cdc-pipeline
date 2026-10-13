UPDATE account a
   SET account_status = CASE r.decision WHEN 'APPROVE' THEN 'ACTIVE' ELSE 'REJECTED' END,
       activated_at   = CASE r.decision WHEN 'APPROVE' THEN r.assessed_at END
  FROM identity_fraud.risk_assessment r
 WHERE r.account_id     = a.account_id
   AND r.decision      IN ('APPROVE', 'DECLINE')
   AND a.account_status = 'PENDING'
RETURNING a.account_id, r.decision, a.account_status, a.activated_at, a.version;

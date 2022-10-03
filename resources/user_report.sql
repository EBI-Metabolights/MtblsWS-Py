select u.id, u.email, u.address, u.affiliation, u.status as user_status, count(*) as total,string_agg(s.acc, ', ' ORDER BY s.acc) as studies,
SUM(CASE s.status WHEN 0 THEN 1 ELSE 0 END) as Submitted,
SUM(CASE s.status WHEN 1 THEN 1 ELSE 0 END) as In_Curation,
SUM(CASE s.status WHEN 2 THEN 1 ELSE 0 END) as In_Review,
SUM(CASE s.status WHEN 3 THEN 1 ELSE 0 END) as Public,
SUM(CASE s.status WHEN 4 THEN 1 ELSE 0 END) as Dormant
from study_user su
JOIN studies s on s.id = su.studyid
JOIN users u on u.id = su.userid
group by  u.id

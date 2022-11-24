select a.*
from ( select t.date,
                case when t1.studies_created is not null then t1.studies_created else 0 end studies_created,
                case when t2.public is not null then t2.public else 0 end                   public_studies,
                case when t4.review is not null then t4.review else 0 end                   review_studies,
                case when t5.curation is not null then t5.curation else 0 end               curation_studies,
                case when t6.user_1 is not null then t6.user_1 else 0 end                   users
         from (select date_1 as date
               from (
                        select date(submissiondate) date_1
                        from studies
                        group by date(submissiondate)
                        union
                        select date(releasedate) date_1
                        from studies
                        group by date(releasedate)
                        union
                        select date(updatedate) date_1
                        from studies
                        group by date(updatedate)
                        union
                        select date(joindate) date_1
                        from users
                        group by date(joindate)
                    ) t1
               group by date_1
               order by date_1) t

                  left join (
             select date(submissiondate) date_1,
                    count(*)                      studies_created
             from studies
             group by date(submissiondate)) t1 on t.date = t1.date_1 -- studies_created

                  left join (
             select date(releasedate) date_1,
                    count(*)                   public
             from studies
             where status = 3
             group by date(releasedate)) t2 on t.date = t2.date_1 -- public

                  left join (
             select date(updatedate) date_1,
                    count(*)                  review
             from studies
             where status = 2
             group by date(updatedate)) t4 on t.date = t4.date_1 -- review

                  left join (
             select date(updatedate) date_1,
                    count(*)                  curation
             from studies
             where status = 1
             group by date(updatedate)) t5 on t.date = t5.date_1 -- curation

                  left join(
             select date(joindate) date_1,
                    count(*)                user_1
             from users
             group by date(joindate)) t6 on t.date = t6.date_1
     ) a -- user_1
where studies_created + public_studies + review_studies + curation_studies+ users > 0
order by date asc;

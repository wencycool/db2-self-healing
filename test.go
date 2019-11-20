package main

import (
	"fmt"
	"regexp"
)

func main() {
	str := `total disk space needed for initial set [  12.696] MB
total disk space constrained to         [1780.033] MB
Trying variations of the solution set.
  1  indexes in current solution
 [225552.8594] timerons  (without recommendations)
 [ 21.1108] timerons  (with current solution)
 [99.99%] improvement


--
--
-- LIST OF RECOMMENDED INDEXES
-- ===========================
-- index[1],   12.696MB
   CREATE INDEX "DB2INST1"."IDX1911201613370" ON "DB2INST1"."TTT"
   ("VARCHAR_COL30" ASC) ALLOW REVERSE SCANS COLLECT SAMPLED DETAILED STATISTICS;
   COMMIT WORK ;
`
	pattern := regexp.MustCompile(`index\[\d+\],\s+(?P<indexSize>)MB\n(?s:.+)(?P<index>CREATE INDEX(?s:.+?)"(?P<idxSchema>.+?)"\."(?P<idxName>.+?)"(?s:.+?)ON(?s:.+?)"(?P<tabSchema>.+?)"\."(?P<tabName>.+?)"(?s:.+?));(?s:.+?)COMMIT WORK`)
	r := pattern.MatchString(str)
	fmt.Println(r)
}

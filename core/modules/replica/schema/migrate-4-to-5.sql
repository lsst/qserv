-- Drop these obsolete columns left over the original implementation of the job classes
ALTER TABLE `job` DROP COLUMN `exclusive` ;
ALTER TABLE `job` DROP COLUMN `preemptable` ;

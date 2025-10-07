ALTER TABLE `transaction` ADD COLUMN `start_time` BIGINT UNSIGNED DEFAULT 0 AFTER `begin_time` ;
ALTER TABLE `transaction` ADD COLUMN `transition_time` BIGINT UNSIGNED DEFAULT 0 AFTER `start_time` ;

UPDATE `transaction` SET `start_time` = `begin_time` ;
UPDATE `transaction` SET `transition_time` = `end_time` ;

CREATE TABLE IF NOT EXISTS `transaction_log` (
  `id`                 INT UNSIGNED    NOT NULL AUTO_INCREMENT,   -- a unique identifier of the event
  `transaction_id`     INT UNSIGNED    NOT NULL ,                 -- FK to the parent transaction
  `transaction_state`  VARCHAR(255)    NOT NULL ,                 -- a state of the transaction tion when the event was recorded
  `time`               BIGINT UNSIGNED NOT NULL ,                 -- an timestamp (milliseconds) when the event was recorded
  `name`               VARCHAR(255)    NOT NULL ,                 -- an identifier of the event
  `data`               MEDIUMBLOB      DEFAULT '' ,               -- optional parameters (JSON object) of the event
  PRIMARY KEY (`id`) ,
  CONSTRAINT `transaction_log_fk_1`
    FOREIGN KEY (`transaction_id`)
    REFERENCES `transaction` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB
COMMENT = 'Events logged in a context of the super-transactions. The information is meant
  for progress tracking, monitoring and performance analysis of the transactions.';

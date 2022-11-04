ALTER TABLE `transaction_contrib`
  ADD COLUMN `max_retries` INT UNSIGNED NOT NULL DEFAULT 0
  AFTER `type`;

ALTER TABLE `transaction_contrib`
  ADD COLUMN `num_failed_retries` INT UNSIGNED NOT NULL DEFAULT 0
  AFTER `max_retries`;

CREATE TABLE IF NOT EXISTS `transaction_contrib_retry` (
  `contrib_id` INT UNSIGNED NOT NULL ,

  `num_bytes`  BIGINT UNSIGNED NOT NULL DEFAULT 0 ,
  `num_rows`   BIGINT UNSIGNED NOT NULL DEFAULT 0 ,
  `start_time` BIGINT UNSIGNED NOT NULL DEFAULT 0 , -- the time the reading/loading input data started
  `read_time`  BIGINT UNSIGNED NOT NULL DEFAULT 0 , -- the time the operation failed

  `tmp_file` TEXT NOT NULL ,  -- the temporary file open to store preprocessed data

  -- Columns for storing the extended info on a problem that prevented a request
  -- from succeeding.
  `http_error`   INT  NOT NULL DEFAULT 0 ,  -- HTTP response code, if applies to a request
  `system_error` INT  NOT NULL DEFAULT 0 ,  -- the UNIX errno captured at a point where a problem occurred
  `error`        TEXT NOT NULL DEFAULT '' , -- the human-readable message

  CONSTRAINT `transaction_contrib_retry_fk_1`
    FOREIGN KEY (`contrib_id`)
    REFERENCES `transaction_contrib` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB
COMMENT = 'Info on the failed retries to pull the input data of the corresponding transaction contributions';

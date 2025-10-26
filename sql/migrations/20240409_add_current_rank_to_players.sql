-- プレイヤーの現在ランクを保持するカラムを追加
ALTER TABLE players
    ADD COLUMN current_rank INT NOT NULL DEFAULT 0 AFTER highest_rank;

-- 追加されたカラムを既存データの最高ランクで初期化
UPDATE players
SET current_rank = GREATEST(current_rank, highest_rank);

/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.apikeys.migration;

import com.nvidia.apikeys.config.NakProperties;
import java.time.Duration;
import java.util.function.LongSupplier;
import lombok.RequiredArgsConstructor;
import net.javacrumbs.shedlock.core.LockAssert;
import net.javacrumbs.shedlock.core.LockExtender;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "apikeys.migration.keys-by-account-index",
        name = "enabled", havingValue = "true", matchIfMissing = true)
public class KeysByAccountIndexMigrationScheduler {
    private final KeysByAccountIndexMigrationTask task;
    private final NakProperties properties;

    @Scheduled(initialDelayString = "${apikeys.migration.keys-by-account-index.initial-delay:PT30M}",
            fixedDelayString = "${apikeys.migration.keys-by-account-index.fixed-delay:PT5M}")
    @SchedulerLock(name = "keys-by-account-index-copy",
            lockAtLeastFor = "${apikeys.migration.keys-by-account-index.lock-at-least-for:PT1S}",
            lockAtMostFor = "${apikeys.migration.keys-by-account-index.lock-at-most-for:PT5M}")
    public void run() {
        LockAssert.assertLocked();
        var config = properties.getMigration().getKeysByAccountIndex();
        var atMost = config.getLockAtMostFor();
        var atLeast = config.getLockAtLeastFor();
        task.run(new LockRenewal(() -> LockExtender.extendActiveLock(atMost, atLeast),
                atMost.dividedBy(3), System::nanoTime));
    }

    static final class LockRenewal implements Runnable {
        private final Runnable extend;
        private final long intervalNanos;
        private final LongSupplier nanoTime;
        private long lastRenewal;
        private boolean renewed;

        LockRenewal(Runnable extend, Duration interval, LongSupplier nanoTime) {
            this.extend = extend;
            this.intervalNanos = interval.toNanos();
            this.nanoTime = nanoTime;
        }

        @Override
        public void run() {
            var now = nanoTime.getAsLong();
            if (!renewed || now - lastRenewal >= intervalNanos) {
                extend.run();
                lastRenewal = now;
                renewed = true;
            }
        }
    }
}

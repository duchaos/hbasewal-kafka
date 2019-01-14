/*
 * Copyright 2012 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.shuidihuzhu.sep;

import java.util.List;

/**
 * Handles incoming Side-Effect Processor messages.
 */
public interface EventListener {

    /**
     * Process a list of events that have been delivered via the Side-Effect Processor (SEP).
     * <p>
     * If an exception is thrown while processing a batch of messages, all messages in the batch will be retried later
     * by the SEP. For this reason, message handling should be idempotent.
     * 
     * @param events contains events representing the HBase update
     */
    void processEvents(List<SepEvent> events);
}

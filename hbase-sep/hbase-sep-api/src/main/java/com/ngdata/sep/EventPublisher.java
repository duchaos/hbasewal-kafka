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
package com.ngdata.sep;


import java.io.IOException;

/**
 * Publisher of side-effect events which are distributed to and handled by {@link EventListener}s.
 */
public interface EventPublisher {

    /**
     * Publish an event to be processed by the side-effect processor (SEP) system.
     * 
     * @param row The row key for the record to which the event is related
     * @param payload The content of the event message
     */
    void publishEvent(byte[] row, byte[] payload) throws IOException;
}

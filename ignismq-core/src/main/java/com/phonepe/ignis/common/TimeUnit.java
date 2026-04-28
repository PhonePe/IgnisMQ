/**
 * Copyright (c) 2026 Original Author(s), PhonePe India Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.phonepe.ignis.common;

/**
 * @author shantanu.tiwari
 */
public enum TimeUnit {
    //Added a validation while taking input, so no need to add the overflow check or using long value.
    MINUTE {
        @Override
        public int toSeconds(int duration) {
            return duration * 60;
        }
    },
    HOUR {
        @Override
        public int toSeconds(int duration) {
            return duration * 60 * 60;
        }
    },
    DAY {
        @Override
        public int toSeconds(int duration) {
            return duration * 24 * 60 * 60;
        }
    };

    public abstract int toSeconds(int duration);
}

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

package com.phonepe.ignis.shovel;

import com.phonepe.ignis.service.AerospikeQueueService;
import com.phonepe.ignis.util.AerospikeTestBase;
import com.phonepe.magazine.Magazine;
import com.phonepe.magazine.entity.MagazineData;
import com.phonepe.magazine.exception.ErrorCode;
import com.phonepe.magazine.exception.MagazineException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

public class ShovelTaskTest extends AerospikeTestBase {

    private Magazine<String> magazine;
    private Magazine<String> sidelineMagazine;
    private AerospikeQueueService queueService;

    @Before
    public void setUp() {
        magazine = Mockito.mock(Magazine.class);
        sidelineMagazine = Mockito.mock(Magazine.class);
        queueService = Mockito.spy(createQueueService());
        when(magazine.getMagazineIdentifier()).thenReturn("TEST_QUEUE");
    }

    @Test
    public void testShovelSuccessfullyMovesMessages() {
        MagazineData<String> data1 = buildMagazineData("msg1");
        MagazineData<String> data2 = buildMagazineData("msg2");

        when(sidelineMagazine.fire()).thenReturn(data1, data2)
                .thenThrow(new MagazineException(ErrorCode.NOTHING_TO_FIRE, "nothing", null));
        when(magazine.load(any())).thenReturn(true);
        doNothing().when(queueService).addFireTimestamp(any(), anyLong());

        ShovelTask task = new ShovelTask(magazine, sidelineMagazine, queueService, false);
        task.run();

        verify(magazine, times(2)).load(any());
        verify(sidelineMagazine, times(2)).delete(any());
    }

    @Test
    public void testShovelReloadsOnFailedLoad() {
        MagazineData<String> data1 = buildMagazineData("msg1");

        when(sidelineMagazine.fire()).thenReturn(data1)
                .thenThrow(new MagazineException(ErrorCode.NOTHING_TO_FIRE, "nothing", null));
        when(magazine.load("msg1")).thenReturn(false);
        when(sidelineMagazine.reload("msg1")).thenReturn(true);
        doNothing().when(queueService).addFireTimestamp(any(), anyLong());

        ShovelTask task = new ShovelTask(magazine, sidelineMagazine, queueService, false);
        task.run();

        // The reload put a fresh copy at the tail of the sideline, so the source record may go.
        verify(sidelineMagazine, times(1)).reload("msg1");
        verify(sidelineMagazine, times(1)).delete(data1);
    }

    @Test
    public void testShovelReloadsOnException() {
        MagazineData<String> data1 = buildMagazineData("msg1");

        when(sidelineMagazine.fire()).thenReturn(data1)
                .thenThrow(new MagazineException(ErrorCode.NOTHING_TO_FIRE, "nothing", null));
        when(magazine.load("msg1")).thenThrow(new RuntimeException("load failed"));
        when(sidelineMagazine.reload("msg1")).thenReturn(true);
        doNothing().when(queueService).addFireTimestamp(any(), anyLong());

        ShovelTask task = new ShovelTask(magazine, sidelineMagazine, queueService, false);
        task.run();

        verify(sidelineMagazine, times(1)).reload("msg1");
        verify(sidelineMagazine, times(1)).delete(data1);
    }

    /**
     * B2: the source record fired out of the sideline is the last copy of the message. If it could
     * not be moved into the main magazine and could not be put back on the sideline either, deleting
     * it destroys the message.
     */
    @Test
    public void testShovelKeepsSourceRecordWhenLoadAndReloadBothFail() {
        MagazineData<String> data1 = buildMagazineData("msg1");

        when(sidelineMagazine.fire()).thenReturn(data1)
                .thenThrow(new MagazineException(ErrorCode.NOTHING_TO_FIRE, "nothing", null));
        when(magazine.load("msg1")).thenReturn(false);
        when(sidelineMagazine.reload("msg1")).thenReturn(false);
        doNothing().when(queueService).addFireTimestamp(any(), anyLong());

        new ShovelTask(magazine, sidelineMagazine, queueService, false).run();

        verify(sidelineMagazine, times(1)).reload("msg1");
        verify(sidelineMagazine, never()).delete(any());
    }

    /**
     * B2: same contract when the fallback reload throws rather than returning false. Against
     * Magazine 2's Aerospike storage this is the reachable failure mode; the boolean rarely is.
     */
    @Test
    public void testShovelKeepsSourceRecordWhenReloadThrows() {
        MagazineData<String> data1 = buildMagazineData("msg1");

        when(sidelineMagazine.fire()).thenReturn(data1)
                .thenThrow(new MagazineException(ErrorCode.NOTHING_TO_FIRE, "nothing", null));
        when(magazine.load("msg1")).thenThrow(new RuntimeException("load failed"));
        when(sidelineMagazine.reload("msg1")).thenThrow(new RuntimeException("reload failed"));
        doNothing().when(queueService).addFireTimestamp(any(), anyLong());

        new ShovelTask(magazine, sidelineMagazine, queueService, false).run();

        verify(sidelineMagazine, never()).delete(any());
    }

    /**
     * A failure on one message must not abandon the rest of the drain.
     */
    @Test
    public void testShovelContinuesAfterAFailedMessage() {
        MagazineData<String> data1 = buildMagazineData("msg1");
        MagazineData<String> data2 = buildMagazineData("msg2");

        when(sidelineMagazine.fire()).thenReturn(data1, data2)
                .thenThrow(new MagazineException(ErrorCode.NOTHING_TO_FIRE, "nothing", null));
        when(magazine.load("msg1")).thenThrow(new RuntimeException("load failed"));
        when(sidelineMagazine.reload("msg1")).thenReturn(false);
        when(magazine.load("msg2")).thenReturn(true);
        doNothing().when(queueService).addFireTimestamp(any(), anyLong());

        new ShovelTask(magazine, sidelineMagazine, queueService, false).run();

        verify(sidelineMagazine, never()).delete(data1);
        verify(sidelineMagazine, times(1)).delete(data2);
    }

    @Test
    public void testShovelNothingToFire() {
        when(sidelineMagazine.fire())
                .thenThrow(new MagazineException(ErrorCode.NOTHING_TO_FIRE, "nothing", null));

        ShovelTask task = new ShovelTask(magazine, sidelineMagazine, queueService, false);
        task.run();

        verify(magazine, never()).load(any());
    }

    @Test
    public void testShovelWithNullDataSkipsLoad() {
        MagazineData<String> nullData = buildMagazineData(null);

        when(sidelineMagazine.fire()).thenReturn(nullData)
                .thenThrow(new MagazineException(ErrorCode.NOTHING_TO_FIRE, "nothing", null));
        doNothing().when(queueService).addFireTimestamp(any(), anyLong());

        ShovelTask task = new ShovelTask(magazine, sidelineMagazine, queueService, false);
        task.run();

        verify(magazine, never()).load(any());
        verify(sidelineMagazine, times(1)).delete(nullData);
    }

    @Test
    public void testShovelWithAutoDeleteOnFatalException() {
        when(sidelineMagazine.fire()).thenThrow(new RuntimeException("fatal"));

        ShovelTask task = new ShovelTask(magazine, sidelineMagazine, queueService, true);
        task.run();

        verify(magazine, never()).load(any());
    }

    @Test
    public void testShovelWithAutoDeleteFalseOnFatalException() {
        when(sidelineMagazine.fire()).thenThrow(new RuntimeException("fatal"));

        ShovelTask task = new ShovelTask(magazine, sidelineMagazine, queueService, false);
        task.run();

        verify(magazine, never()).load(any());
    }

    @Test
    public void testShovelFireNonNothingToFireException() {
        when(sidelineMagazine.fire())
                .thenThrow(new MagazineException(ErrorCode.INTERNAL_ERROR, "some error", null));

        ShovelTask task = new ShovelTask(magazine, sidelineMagazine, queueService, false);
        task.run();

        verify(magazine, never()).load(any());
    }

    @Test
    public void testShovelRetriesExhaustedDoesNotDeleteData() {
        when(sidelineMagazine.fire())
                .thenThrow(new MagazineException(ErrorCode.RETRIES_EXHAUSTED, "data may remain", null));

        new ShovelTask(magazine, sidelineMagazine, queueService, false).run();

        verify(magazine, never()).load(any());
        verify(sidelineMagazine, never()).delete(any());
    }

    private <T> MagazineData<T> buildMagazineData(final T data) {
        return MagazineData.<T>builder()
                .magazineIdentifier("TEST_QUEUE_SIDELINE")
                .shard(1)
                .data(data)
                .firePointer(100)
                .build();
    }
}

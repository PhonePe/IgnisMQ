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

package com.phonepe.ignis;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAPackage;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noMethods;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;

public class ArchitectureTest {

    private static final JavaClasses CLASSES = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("com.phonepe.ignis");

    @Test
    public void dropwizardAndDropwizardMetricsStayOutOfCore() {
        noClasses()
                .should().dependOnClassesThat()
                .resideInAnyPackage("io.dropwizard..", "com.codahale.metrics..")
                .because("ignismq-core is framework-free; the Dropwizard bridge belongs to the bundle")
                .check(CLASSES);
    }

    @Test
    public void nothingSchedulesWorkOnAJavaUtilTimer() {
        noClasses()
                .that().resideOutsideOfPackage("com.phonepe.ignis.scheduler")
                .should().dependOnClassesThat()
                .haveFullyQualifiedName("java.util.Timer")
                .orShould().dependOnClassesThat().haveFullyQualifiedName("java.util.TimerTask")
                .because("scheduling goes through IgnisSchedulerCommands, which survives a throwing task "
                        + "and can actually be shut down")
                .check(CLASSES);
    }

    @Test
    public void valueTypesDoNotDependOnBehaviour() {
        noClasses()
                .that().resideInAnyPackage("com.phonepe.ignis.entity", "com.phonepe.ignis.common",
                        "com.phonepe.ignis.config", "com.phonepe.ignis.metric")
                .should().dependOnClassesThat()
                .resideInAnyPackage("com.phonepe.ignis", "com.phonepe.ignis.service..",
                        "com.phonepe.ignis.consumer..", "com.phonepe.ignis.shovel..",
                        "com.phonepe.ignis.sweep..", "com.phonepe.ignis.leadership..",
                        "com.phonepe.ignis.client..")
                .because("entities, config and metrics are values, not participants")
                .check(CLASSES);
    }

    @Test
    public void aerospikeTypesStayInStorageServiceAndClient() {
        noClasses()
                .that().resideOutsideOfPackage("com.phonepe.ignis.service..")
                .and().resideOutsideOfPackage("com.phonepe.ignis.client..")
                .and().resideOutsideOfPackage("com.phonepe.ignis.storage..")
                .should().dependOnClassesThat().resideInAnyPackage("com.aerospike..")
                .because("storage internals must not leak into queues, consumers or the sweeper")
                .check(CLASSES);
    }

    @Test
    public void theManagerDoesNotExposeMagazineTypes() {
        noMethods()
                .that().areDeclaredIn(IgnisMQManager.class)
                .and().arePublic()
                .should().haveRawReturnType(resideInAPackage("com.phonepe.magazine.."))
                .because("Magazine is an implementation detail; users hold IQueue and QueueStat")
                .check(CLASSES);
    }

    @Test
    public void queuesDoNotExposeMagazineTypesPublicly() {
        noMethods()
                .that().areDeclaredIn(MagazineQueue.class)
                .and().arePublic()
                .should().haveRawReturnType(resideInAPackage("com.phonepe.magazine.."))
                .because("the sweeper reaches these package-privately; users must not reach them at all")
                .check(CLASSES);
    }

    @Test
    public void packagesAreFreeOfCycles() {
        slices()
                .matching("com.phonepe.ignis.(**)")
                .should().beFreeOfCycles()
                .check(CLASSES);
    }
}

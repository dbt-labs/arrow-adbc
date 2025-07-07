# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

MANAGERS = bigquery databricks flightsql panicdummy snowflake

GO_BUILD := go build
RM := rm -f

ifeq ($(shell go env GOOS),linux)
	SUFFIX=so
else ifeq ($(shell go env GOOS),windows)
	SUFFIX=dll
	RM=del
else
	SUFFIX=dylib
endif

# Expand dynamically libadbc_driver_.SUFFIX
DRIVERS := $(addsuffix .$(SUFFIX),$(addprefix libadbc_driver_,$(MANAGERS)))

# =========
#  Targets
# =========

.PHONY: all regenerate
all: $(DRIVERS)


libadbc_driver_%.$(SUFFIX): % ../driver/% ../go.mod ../go.sum
	$(GO_BUILD) -tags driverlib -o $@ -buildmode=c-shared -ldflags "-s -w" ./$(*)
	$(RM) $(basename $@).h

regenerate:
	go run gen/main.go -prefix BigQuery -o ./bigquery/ -driver ../driver/bigquery
	go run gen/main.go -prefix Databricks -o ./databricks/ -driver ../driver/databricks
	go run gen/main.go -prefix FlightSQL -o ./flightsql/ -driver ../driver/flightsql
	go run gen/main.go -prefix PanicDummy -o ./panicdummy/ -driver ../driver/panicdummy
	go run gen/main.go -prefix Snowflake -o ./snowflake/ -driver ../driver/snowflake

clean:
	$(RM) $(DRIVERS)

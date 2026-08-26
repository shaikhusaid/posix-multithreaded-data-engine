CXX     = g++
CXXFLAGS = -Wall -Wextra -pthread -g -Isrc
LDFLAGS  = -pthread -lrt

SRCS_DIR  = src
COMMON_H  = $(SRCS_DIR)/common/common.h

TARGETS = dispatcher ingester processor reporter

.PHONY: all clean

all: $(TARGETS)

dispatcher: $(SRCS_DIR)/dispatcher.cpp $(COMMON_H)
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

ingester: $(SRCS_DIR)/ingester.cpp $(COMMON_H)
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

processor: $(SRCS_DIR)/processor.cpp $(COMMON_H)
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

reporter: $(SRCS_DIR)/reporter.cpp $(COMMON_H)
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

clean:
	rm -f $(TARGETS)
	rm -f logs/*.log
	rm -f output/report.txt output/report.csv
	rm -f /tmp/os_proj_fifo
	ipcrm -M 5678 2>/dev/null || true
	@echo "Cleaned."

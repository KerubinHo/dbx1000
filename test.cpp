#include <algorithm>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <map>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <sys/types.h> /* for pid_t */
#include <sys/wait.h>  /* for wait */
#include <unistd.h>    /* for fork */
#include <vector>

using namespace std;

int main() {
  vector<double> zipf = {0, 0.2, 0.4, 0.6, 0.8, 1.01, 1.2, 1.4};
  vector<int> scan = {5, 10, 15, 20};
  vector<int> scan_len = {10, 20, 30};
  vector<int> length = {10, 20, 30};
  vector<double> read = {40, 50, 60, 70};
  vector<string> cc = {"hstore",    "silo",    "no_wait", "mvcc", "wait_die",
                       "dl_detect", "hekaton", "occ",     "vll",  "tictoc"};

  ofstream out("scan-train.out");
  ofstream stat("scan-stat.out");
  int count = 0;
  for (size_t l = 0; l < scan_len.size(); l++) {
    for (size_t s = 0; s < scan.size(); s++) {
      for (size_t r = 0; r < read.size(); r++) {
        for (size_t R = 0; R < length.size(); R++) {
          for (size_t z = 0; z < zipf.size(); z++) {
            map<double, int> comp;
            string temp;
            stringstream ss(temp);
            vector<vector<string>> stats(10);
            for (size_t c = 1; c < cc.size(); c++) {
              pid_t pid = fork();
              if (pid == 0) {
                static char *argv[] = {
                    const_cast<char *>(cc[0].c_str()),
                    const_cast<char *>(
                        string("-r" + to_string(read[r] / 100.0)).c_str()),
                    const_cast<char *>(
                        string("-s" + to_string(scan_len[r])).c_str()),
                    const_cast<char *>(
                        string("-w" + to_string(1.0 - (read[r] / 100.0) -
                                                (scan[s] / 100.0)))
                            .c_str()),
                    const_cast<char *>(string("-e" + to_string(1)).c_str()),
                    const_cast<char *>(
                        string("-R" + to_string(length[R])).c_str()),
                    const_cast<char *>(
                        string("-z" + to_string(zipf[z])).c_str()),
                    NULL};
                execv(string("./" + cc[c]).c_str(), argv);
                exit(127);
              } else {
                waitpid(pid, 0, 0);
              }
              ifstream file(cc[c] + ".dat");
              istream_iterator<string> is(file);
              istream_iterator<string> eos;

              copy(is, eos, back_inserter(stats[c]));

              if (c == 1) {
                ss << count << "\t" << 100 << "\t" << 2 << "\t" << 0 << "\t"
                   << zipf[z] << "\t" << length[R] << "\t" << read[r] << "\t" << scan[s] << "\t" << scan_len[l];
                out << count << "\t" << 100 << "\t" << 2 << "\t" << 0 << "\t"
                    << zipf[z] << "\t" << length[R] << "\t" << read[r] << "\t" << scan[s] << "\t" << scan_len[l];
                stat << count << "\t" << 100 << "\t" << 2 << "\t" << 0 << "\t"
                     << zipf[z] << "\t" << length[R] << "\t" << read[r] << "\t" << scan[s] << "\t" << scan_len[l];
              }
              stat << "\t" << stats[c][7];
              comp[stod(stats[c][7])] = c;
            }
            stat << endl;
            auto first = comp.rbegin();
            auto second = next(first, 1);
            for (size_t i = 0; i < stats[first->second].size() - 1; i++) {
              ss << "\t" << stats[first->second][i];
              out << "\t" << stats[first->second][i];
            }
            // bool check = false;
            ss << "\t" << first->second;
            out << "\t" << first->second;
            for (auto it = second; it != comp.rend(); it++) {
              if (it->first >= (0.95 * first->first)) {
                // check = true;
                ss << "\t" << it->second;
                out << "\t" << it->second;
              }
            }
            ss << endl;
            out << endl;
            cout << ss.str();
            count++;
            // cout << min << " " << e << " " << max << endl;
          }
        }
      }
    }
  }
  return 0;
}

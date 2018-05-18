#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <sys/types.h> /* for pid_t */
#include <sys/wait.h>  /* for wait */
#include <unistd.h>    /* for fork */
#include <vector>
#include <iostream>
#include <fstream>
#include <iterator>
#include <algorithm>
#include <sstream>
#include <iomanip>
#include <map>

using namespace std;

int main() {
  vector<double> zipf = {0, 0.4, 0.8, 1.01, 1.2, 1.4, 1.5};
  vector<int> length = {10, 15, 20, 25};
  vector<double> read = {50, 70, 90, 95, 100};
  vector<string> cc = {"hstore", "silo", "no_wait"};

  ofstream out("pcc-train.out");
  int count = 0;
  for (size_t r = 0; r < read.size(); r++) {
    for (size_t R = 0; R < length.size(); R++) {
      for (size_t z = 0; z < zipf.size(); z++) {
        int e = 0;
        int min = 0;
        int max = 100;
        while (max - min > 1) {
          map<double, int> comp;
          string temp;
          stringstream ss(temp);
          for (size_t c = 0; c < cc.size(); c++) {
            pid_t pid = fork();
            if (pid == 0) {
              static char *argv[] = {
                  const_cast<char *>(cc[0].c_str()),
                  const_cast<char *>(
                      string("-r" + to_string(read[r]/ 100.0)).c_str()),
                  const_cast<char *>(
                      string("-w" + to_string(1.0 - (read[r] / 100.0))).c_str()),
                  const_cast<char *>(
                      string("-e" + to_string((double)e / 100.0)).c_str()),
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

            vector<string> stats;
            copy(is, eos, back_inserter(stats));

            if (c == 0) {
              ss << count << "\t" << e << "\t" << 2 << "\t" << 0 << "\t" << zipf[z] << "\t" << length[R] << "\t" << read[r];
              out << count << "\t" << e << "\t" << 2 << "\t" << 0 << "\t" << zipf[z] << "\t" << length[R] << "\t" << read[r];
              for (size_t i = 0; i < stats.size() - 1; i++) {
                ss << "\t" << stats[i];
                out << "\t" << stats[i];
              }
            }
            comp[stod(stats[7])] = c;
          }
          auto first = comp.rbegin();
          auto second = next(first, 1);
          //bool check = false;
          ss << "\t" << first->second;
          out << "\t" << first->second;
          if (second->first >= (0.95 * first->first)) {
            //check = true;
            ss << "\t" << second->second;
            out << "\t" << second->second;
          }
          ss << endl;
          out << endl;
          cout << ss.str();
          if (first->second == 0/* || (check && (second->second == 0))*/) {
            min = e;
            e = (min + max) / 2;
          } else {
            max = e;
            e = (min + max) / 2;
          }
          count++;
          //cout << min << " " << e << " " << max << endl;
        }
      }
    }
  }
  return 0;
}

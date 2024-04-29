ssh erdemh@remote11.chalmers.se
ssh erdemh@bayes.ita.chalmers.se

mkdir assignment_problem1
cd assignment_problem1

wget https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-6.7.5.tar.xz
tar -xvf linux-6.7.5.tar.xz

du -sh linux-6.7.5.tar.gz
du -sh linux-6.7.5

find linux-6.7.5 -type f -name "*.c" | wc -l
find linux-6.7.5 -type f -name "*.h" | wc -l

find linux-6.7.5 \( -name '*.h' -o -name '*.c' \) -exec wc -l {} + | sort -nr | grep -v "total$" | head -10 | awk '{print $2}' > longest.txt

xargs sha1sum < longest.txt > longest.sha1sum

grep -lr "Linus Torvalds" linux-6.7.5/ | sort -u | wc -l

find linux-6.7.5 -type f -name "*.c" -exec grep -l "SPDX-License-Identifier" {} + | xargs -I {} sh -c 'echo {} $(grep "SPDX-License-Identifier" {} | cut -d " " -f 3) >> licenses.txt'
awk '{print $2}' licenses.txt | sort | uniq -c | sort -nr | head -n 1

ssh -L 2222:bayes.ita.chalmers.se:22 erdemh@remote11.chalmers.se C:\Users\erdem\Desktop
scp -P 2222 erdemh@localhost:~/assignment_problem1/longest.txt C:\Users\erdem\Desktop
scp -P 2222 erdemh@localhost:~/assignment_problem1/longest.sha1sum C:\Users\erdem\Desktop
scp -P 2222 erdemh@localhost:~/assignment_problem1/licenses.txt C:\Users\erdem\Desktop

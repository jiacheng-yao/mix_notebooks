a = [(1, 2), (4, 5), (5, 9), (8, 10), (2, 3)]

a.sort()

max_itvl = [a[0]]
tmp_idx = 0

for i in range(1, len(a)):
    if max_itvl[tmp_idx][1] < a[i][0]:
        tmp_idx = tmp_idx + 1
        max_itvl.append(a[i])
        continue
    else:
        max_itvl[tmp_idx] = (max_itvl[tmp_idx][0], a[i][1])

max_itvl_len = max_itvl[0][1] - max_itvl[0][0]
max_itvl_idx = 0
for i in range(1, len(max_itvl)):
    if max_itvl_len < max_itvl[i][1] - max_itvl[i][0]:
        max_itvl_len = max_itvl[i][1] - max_itvl[i][0]
        max_itvl_idx = i

print max_itvl[max_itvl_idx]
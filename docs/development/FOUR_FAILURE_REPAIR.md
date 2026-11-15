# 四项综合验收失败修复记录

更新：2026-11-15。当前c411/122py：core18为2134 PASS/52 SKIP/31 DESELECTED/158.30秒，worker19为110 PASS/零skip/284.07秒，images18同源。focus22三任务completed并取得必要checks/reviews，原raw1 PASS/2 NEEDS_REVIEW保留，未统一3/3；同源无过滤full22已正式30/30 PASS，必要复核和最终零付费汇总完成。用户累计11.65的Owner审计282332已实际应用，原50/rev1及unknown held不变；本轮只保留必要最终校验，不再逐阶段扩写历史。

## 版本与当前证据

| 阶段 | 版本 | 已取得结果与边界 |
| --- | --- | --- |
| 修复前基线 | 855dd094；既有实现源25b1be61… | 原matrix06执行源ee73a730…复核为26 PASS、4 FAIL/30，属于历史证据 |
| 第一轮修复 | 提交13dcf7f，源`b844d3b83ae97514120fde00e3a200088f1d674a7a9ce0ca8735fad19453b430`，122py | 新真实matrix07仅记录7项后停止；worker02为98 PASS、231.25秒，不能移作最终候选结果 |
| 08阶段候选 | 提交a525819，源`d74a0e0c49c889cdda6d9189b7db3e895f14f818384aaaa730baf37914a87487`，122py | repair-core-02为1830 PASS、51 SKIP、29 DESELECTED，123.63秒；mypy122/Ruff/format通过。后续改源不继承这些结果 |
| 08阶段容器复验 | d74a两镜像；repair-workers-03 | 98 PASS、零skip、249.88秒；不沿用b844，也不提前移作下一源码证明 |
| 固定矩阵08 | d74a，固定分母30，stop-on-failure | 首FAIL自动停止；前6条原始结果4 PASS/1 NEEDS_REVIEW/1 FAIL，不是完整30项通过 |
| 09阶段records候选 | 提交d64cbfa，源`3896cee3eda0fce7151059870f7498092095036d162f70292a1a28c1c9676c0c`，122py | repair-core-03为1844 PASS、51 SKIP、29 DESELECTED，119.66秒；静态通过。只归该阶段，后续改源不继承 |
| records定向合同 | 同3896 | 31项本地PPTX字节/网关纯回归、28项真实PG Toolkit合同通过；不算完整矩阵或供应商任务通过数 |
| 09阶段容器复验 | 3896两镜像；repair-workers-04 | 98 PASS、零skip、232.31秒；新实证不沿用d74a的98/249.88秒 |
| matrix09 | 3896/d64cbfa；固定分母30 | 首FAIL自动止；前4条2 PASS/1 NEEDS_REVIEW/1 FAIL，未完成30项 |
| 10阶段metadata候选 | 提交89d69a9，源`321e75bd56562c79fbcbc29c6c5cc96ba85eb8ac7bb775ff08e90d449fac4002`，122py | repair-core-04为1860 PASS、51 SKIP、29 DESELECTED，141.70秒；mypy122/Ruff/format通过 |
| metadata定向合同 | 同321e | 26项metadata纯回归、39项projection/工具真实PG合同通过，不替代真实矩阵 |
| 10阶段容器复验 | 321e两镜像；repair-workers-06 | 98 PASS、零skip、227.23秒；worker05因DSN URI配置格式误生成原生停止，48条已过不计最终 |
| matrix10 | 321e/89d69a9；固定分母30 | 首FAIL自动止；前3条1 PASS/1 NEEDS_REVIEW/1 FAIL（DOCX03），PPTX尚未执行 |
| 11阶段引用标记/retry候选 | 提交0a09756，源`d4926c65e3b5995187d9e88347d89fc65a1ed80dda34d90013a676b3ccf7b632`，122py | repair-core-05为1893 PASS、51 SKIP、29 DESELECTED，123.60秒；mypy122/Ruff/format通过。新增纯26、真实PG工具56、执行54通过 |
| 11阶段容器复验 | d492两镜像；repair-workers-07 | 98 PASS、零skip、240.29秒；该阶段实测，不继承321e及更早的worker结果 |
| matrix11 | d492/0a09756；固定分母30 | 首FAIL自动止；前6条4 PASS/1 NEEDS_REVIEW/1 FAIL（PPTX03 tokenbudget） |
| 12阶段原生表格结构候选 | 提交7f26d3a，源`947c92cb0adb6c73d598093f77d6a23c40dbbc707a8427f5f5f513fd29918276`，122py | repair-core-06为1913 PASS、51 SKIP、29 DESELECTED，152.49秒；mypy122/Ruff/format通过；原生结构纯37与真实PG工具37通过 |
| 12阶段容器复验 | 947c两镜像；repair-workers-08 | 98 PASS、零skip、258.25秒；该阶段实测，不继承d492或更早结果 |
| matrix12 | 947c/7f26d3a；固定分母30 | 第16项CHART02首FAIL自动止；前16条14 PASS/1 NEEDS_REVIEW/1 FAIL |
| 13阶段图表标签可读性候选 | 源码提交4b943d6，源`8f5a5c261c1a6976dd072401e46730f3b989889f4afea341912099a1b4a882bf`，122py；测试修正ede7ebb未改源 | repair-core-07为1923 PASS、51 SKIP、31 DESELECTED，149.00秒；mypy122/Ruff/format通过，该次冻结时原12图/gold/checker/expected labels未改 |
| 13阶段容器证据 | 8f5a两images07；repair-workers-09与定向复验 | 单次全suite为107 PASS/2 FAIL；两项测试误读OCR pages envelope，测试改读pages[0]后真实目标2 PASS/6.72秒。合计109项有通过证据，不能称单次109全绿 |
| runtime-focus-13 | 8f5a/images07；原4失败及调度依赖共6项 | results revision1为6/6 PASS；DOCX02/WEB01经独立assistant语义复核后零费用recheck。未选24项仍NOT_RUN，不补入完整矩阵 |
| runtime-matrix-13 | 单源8f5a/images07；固定分母30 | `summary-operator-stopped.json`最终29 PASS/1 FAIL/0 NEEDS_REVIEW；含本轮4份独立assistant review与1次真实OCR侧录。EDIT02输入准备暂停，非30 PASS |
| 14阶段输入scope候选 | 提交c5ccd29，源`293d236e9ba03843b1e93a5c12244f1b421f6f68ff42cd883a9dc96e3353364f`，122py | repair-core-08为1936 PASS、51 SKIP、31 DESELECTED，142.77秒；mypy122/Ruff/format通过；新增artifact-primary-provenance 12 PG与旧input 20 PG通过 |
| checker诊断修复 | 新指纹`0664a446…`；gold仍`6c7c635a…` | None comparison诊断健壮性38项纯回归通过，PASS阈值不变；旧`68d49f…`归档，不把缺结果算PASS |
| 14阶段容器复验 | 293d两images08；repair-workers-10 | 单次109/109 PASS、零skip、269.98秒；该阶段实测，不移用到下一源 |
| runtime-focus-14 | 293d/images08；原4失败及EDIT02依赖共8项 | selected8为7 PASS/1 FAIL（WEB01）；EDIT02实际completed/PASS，70.42秒 |
| runtime-matrix-14 | 293d/images08；固定分母30 | **NOT_RUN**；未启动，不补作通过 |
| 15阶段research质量候选 | 提交8da0a24，源`a24f55c313e1f322d66f8c554e0f19d8ab99d91d26b8b5de2a0f80e51c7e2d88`，122py；freeze09 | repair-core-09为1938 PASS、51 SKIP、31 DESELECTED，138.06秒；mypy122/Ruff/format与研究PG30通过，不继承293d结果 |
| 15阶段容器复验 | a24f两images09；repair-workers-11 | 单次109/109 PASS、零skip、260.75秒；不移用到后续源码 |
| runtime-focus-15 | a24f/images09；selected8 | 前2条DOCX02 NEEDS_REVIEW/PPTX01 FAIL（建Work前clarify）；后6条NOT_RUN |
| runtime-matrix-15 | a24f/images09；固定分母30 | **NOT_RUN**；未启动，不拼历史13结果 |
| 16阶段严格逐页PPT候选 | 提交7288149，源`bd7459c1f41ec0e67694c7a3cff4152a017dcc2869b561d77e1b8d354ca5192a`，122py；freeze10 | repair-core-10为1984 PASS、51 SKIP、31 DESELECTED，144.26秒；mypy122/Ruff/format及Root diff检查通过，纯98/PG18通过，不移用a24f结果 |
| 16阶段容器复验 | bd74两images10；repair-workers-12 | 单次109/109 PASS、零skip、264.21秒；不移用到下一源 |
| runtime-focus-16 | bd74/images10；selected8，仅前7项执行 | 固定30最终6 PASS/1 FAIL（SCHEDULE03 waiting_input）/23 NOT_RUN、0 NEEDS_REVIEW；DOCX/WEB本轮新review PASS，WEB无多余示例 |
| runtime-matrix-16 | bd74/images10；固定分母30 | **NOT_RUN**；未启动，不拼历史结果 |
| 17阶段可信纯提醒目录候选 | 提交934ccb3，源`c97bf2f784ca81918d567a6b6d21941d6406ec821eed07d8da62af068d019d1b`，122py；freeze11 | repair-core-11为1984 PASS、51 SKIP、31 DESELECTED，145.45秒；mypy122/Ruff/format及Root diff通过，纯81/综合PG126通过，不继承bd74结果 |
| 17阶段容器复验 | c97b两images11；repair-workers-13 | 单次109/109 PASS、零skip、250.05秒；不移用到下一源 |
| runtime-focus-17 | c97b/images11；原4失败及EDIT02依赖共8项 | selected8/8 PASS、0 FAIL、0 NEEDS_REVIEW；含本次DOCX/WEB独立语义review+零费用recheck，未选22项NOT_RUN，不借旧focus16 |
| runtime-matrix-17 | c97b/images11；固定分母30 | 第6项PPTX03路由reply因provider_unavailable/confidence0首FAIL止；完整30未通过，精确最终汇总/账本待提供 |
| 18阶段Host纯推理retry候选 | 提交09af4ea，源`6e4ba3eb03894272a7cd544ea0eb0b56b7c3265bb1da603becd9e882ff9a4ea3`，122py；freeze12 | repair-core-12为2012 PASS、51 SKIP、31 DESELECTED，161.42秒；mypy122/Ruff/format及Root diff通过，共享58 PG合同PASS，域回归计数不相加成矩阵任务数 |
| 18阶段容器复验 | 6e4b两images12；repair-workers-14 | 单次109 PASS、268.53秒；不移用到下一源 |
| runtime-focus-18 | 6e4b/images12；selected8 | `summary-stopped-web01.json`最终7 PASS/1 FAIL；零付费recheck前后账本不变，原NR/原答保留在revisions/0001 |
| runtime-matrix-18 | 6e4b/images12；固定分母30 | **NOT_RUN**；从未启动，不是正在执行或完整通过 |
| 19阶段研究内部计量合同 | 提交6391bff369d194c1af41cd1605a94910fd52f53b，源`ca7bd28dd057e9a2f1093b316ba1de5efbddd6ec9620f40e0e80ae16ca47043c`，122py；freeze15 | repair-core-15为2060 PASS、51 SKIP、31 DESELECTED、2旧warning，164.53秒；mypy122/Ruff/format/Root diff通过，38 pure/42真实PG合同通过（含10新PG） |
| 19阶段容器复验 | ca7b两images15；repair-workers-16 | 单次109 PASS、零skip、267.75秒；不借到后续source |
| runtime-focus-19 | ca7b/images15；仅WEB01一项 | 正式summary-final固定30=1 PASS/29 NOT_RUN；本轮独立assistant语义review+零付费recheck，不拼旧7项 |
| runtime-matrix-19 | ca7b/images15；无过滤固定30 | `summary-semantic-stop.json`正式26 PASS/2 FAIL/2 NOT_RUN；WEB02语义FAIL，另1 FAIL为SCHEDULE03操作中断原生cancelled；2 EDIT未始，不当30通过 |
| 20阶段research证据/事实合同 | 提交6cbf7515cf026b1c4e6f4165db5c3a3501eaccbd，源`4e36c402d9ab2bcccc68d6c6a6f5d7bc36e08ecf34321093a5faa7e1d5c6df58`，122py；freeze16 | repair-core-16为2068 PASS、51 SKIP、31 DESELECTED、2旧warning，184.72秒；mypy122/Ruff/format281/Root diff通过，42 pure+44真实PG合同共86 PASS（夹具，不代表实模效果） |
| 20阶段容器复验 | 4e36两images16；repair-workers-17 | 实际109 PASS、零skip、271.23秒；不移用到fae5 |
| runtime-focus-20 | 4e36/images16；仅WEB01+WEB02两项 | 零付费复核正式2/2 PASS，固定30 summary为2 PASS/28 NOT_RUN；本轮新非human独立review、scope/answer SHA绑定，不拼旧PASS |
| runtime-matrix-20 | 4e36/images16；无过滤固定30 | `summary-stopped-web01.json`正式20 PASS/1 FAIL/9 NOT_RUN、0 NEEDS_REVIEW；WEB01安全暂停token_budget_exhausted，active=[]，未重写旧答或借其他run |
| 21阶段原请求交付/权限合同 | 提交c861b9ad3bfebc747458fae0e6c19bfb424cca38，源`fae5c989ed759b38cd5c14412edb08f8b7d47f65f1d157b9fd9c97e69276440a`，122py；freeze17 | repair-core-17为2100 PASS、51 SKIP、31 DESELECTED、2旧warning，159.42秒；mypy122/Ruff/format282/Root diff通过，新增32（23pure/9PG）及相关256 mixed回归PASS，不写256PG或实模效果 |
| 21阶段容器复验 | fae5两images17；repair-workers-18 | 实际109 PASS、零skip、257.87秒；不移用到c411 |
| runtime-focus-21 | fae5/images17；仅WEB01+WEB02两项 | 本轮fresh 2 review+零付费正式复核2/2 PASS，固定30=2 PASS/28 NOT_RUN；不拼旧cases |
| runtime-matrix-21 | fae5/images17；无过滤固定30 | 正式15 PASS/1 FAIL/14 NOT_RUN；CHART02原图/3vision正确，原PNG29b…与规范化view d67…误比后修复/预算暂停，未交付 |
| 当前图像绑定/Owner账单合同 | media6b290fc71ac177953a5d991b649471780db9a891、billing b40eeb93f616c67beb1f3db5bf5af9daf61621c2；源`c411860f470f9cb398177979185e9a8ea154275c571d07ecc38b7a5f6935cfc5`，122py；freeze18 | repair-core-18为2134 PASS、52 SKIP、31 DESELECTED、2旧warning，158.30秒；mypy122/Ruff/format285/Root diff通过，billing105（18新）/媒体定向（8新PG）及独立双review通过 |
| 当前容器复验 | c411两images18；repair-workers-19 | 实际110 PASS、零skip、284.07秒；含新增真实worker用例，覆盖新增第52项core skip，不把其他skip当能力通过 |
| runtime-focus-22 | c411/images18；仅CHART02+WEB01+WEB02三项 | 三任务completed；CHART02全11硬checks/原像素绑定通过，双WEB本轮sidecar真PASS；原raw1 PASS/2 NEEDS_REVIEW保留，按加速策略未统一3/3 recheck |
| runtime-matrix-22 | c411/images18；无过滤固定30 | 新run已启动，**PENDING最终结果**；只在最终做一次完整零付费汇总，不复用focus/旧case结果 |
| 17阶段export13 | tree`8fcf5d06eed074f7b7260302d25bbb6f43845822` | 341个tracked文件，Gitleaks 0命中、二进制0；仅该阶段快照 |
| 代码/checker export20 | tree`af9a016d73031eaacf621e7d2fcce4895f694afb` | 347个tracked文件，Gitleaks 0命中、二进制0；等价remote对象`4728789aa1270772c0b62b9907141abb812962a2`仅准备、未移动ref，不等于最终docs导出或CI |
| 当前Python依赖audit | `runtime/ci/repair-python-audit-final.json`，exit0 | 环境65包，其中groupagent本体未上PyPI跳过1；64个可审计依赖0已知漏洞，不写作65包全部审计，不覆盖OS/SDK/目标服务器 |
| 当前候选包快照 | `runtime/ci/repair-package-content-04.json` | wheel149 entries、sdist349 entries，private_entries均0；包内全部122py逐文件匹配freeze18/sourcec411，不把04哈希当最终docs/head包哈希 |
| 发布检查 | 最新提交CI | 此前855阶段已绿的历史证明保留；最新提交状态以[PR #8 checks](https://github.com/huchenzi0609-creator/GroupAgent/pull/8/checks)为准，不预判未观测head |

08阶段core证据为`runtime/ci/repair-core-02.log/.xml`；b844容器证据为`runtime/ci/repair-workers-02.log/.xml`。两阶段冻结记录`source-freeze-01.json`和`source-freeze-02.json`保存在`runtime/repair-20261115/`，不当作当前候选的新测量。

d74a的`runtime/ci/repair-workers-03.log/.xml`记录98 PASS/249.88秒；同源证明`runtime/build/frozen-image-source-proof-repair-02.json`绑定worker `sha256:6fa557f9e67808d66d2f6ee5352fbe67c61a1fc4918feb11cffb0d4c39e75267`及Harness `sha256:e8643f6e852aa23d5e0db3c55c275f9860734c91dfaac87e9e9fead0e2246e32`。schema001—012未变，SDK/runtime固定0.2.2rc2依赖层复用，不称native独立重现。

09阶段3896的`runtime/build/frozen-image-source-proof-repair-03.json`绑定worker `sha256:19ca99e1850fa97c88c2034f109ba9b55e391d10c74af85283b27c5a23c47699`与Harness `sha256:5b7074f19913a59758317875bf57bfe871ef4846016bbea1f9a3258fc37ede14`，同源122py；worker04为98 PASS/232.31秒、core03为1844 PASS，均仅归该阶段，旧core02仅归d74a。

10阶段321e的`runtime/build/frozen-image-source-proof-repair-04.json`绑定worker `sha256:099646d30c3005d9691c966e7214d1caa3b1fb146a904ab0d77ecbd8f4266b7d`及Harness `sha256:b1288f9058d0bdea56db648388beabcba59903bfa18bcedf7066bc784322f940`，同源122py。该阶段证据为`runtime/ci/repair-core-04.log/.xml`、`repair-workers-06.log/.xml`；不移用到当前候选，固定SDK依赖层复用、不称native独立重现。

11阶段d492的`runtime/build/frozen-image-source-proof-repair-05.json`确认两镜像122py匹配同源，worker `sha256:26dc5a1e423e08b1073d70875f68c18fb0df3afb85d53d739ede72cfa462be31`、Harness `sha256:219902f56eb2a4c1635b3bc4f16473ab56346caf2ea198ad7f24e07d7e4465ae`；core证据为`runtime/ci/repair-core-05.log/.xml`，worker07证据为`runtime/ci/repair-workers-07.log/.xml`，98 PASS/零skip/240.29秒。后续改源不继承；依赖层复用边界和schema001—012保持不变。

12阶段947c的`runtime/build/frozen-image-source-proof-repair-06.json`确认两镜像122py匹配同源：worker `sha256:a76f1658c71177f281d768bf2836bab8916e1bf18982044667ec02ada35c72a1`、Harness `sha256:3bf125805dfa1d03d57d2d6a89918c2f6ebe7df802f7a1740f9cd624a8970f78`。core证据为`runtime/ci/repair-core-06.log/.xml`；worker08证据为`runtime/ci/repair-workers-08.log/.xml`，98 PASS/零skip/258.25秒。后续改源不继承，不移用d492的98项结果。

13阶段8f5a两images07同源122py：worker `sha256:4250b11c7d835dab283dc20f4b8e78e27bbe57cec4e30adba5cb07cfd9303457`、Harness `sha256:ab67bc46b09b2a4d1eaf2a13315acfcc4a01108efbcec99a67ae47349ebe3865`。worker09的107 PASS/2 FAIL原日志保留；这2项是测试误读worker OCR的pages envelope，并非产品或OCR失败。ede7ebb只修测试使用pages[0]，实际目标复验2 PASS/6.72秒。不把这两批证据转作293d单次109全绿。

14阶段293d的`runtime/build/frozen-image-source-proof-repair-08.json`确认两images08同源122py：worker `sha256:fee774dd29104cda3425f0d1a53654a8dcf83c7f17f71160226b4bb749e1767f`、Harness `sha256:71682556957eb30d6b4ac58fce113d1c1ebbca66d0fc793a6a1d936f9a620309`。core证据为`runtime/ci/repair-core-08.log/.xml`；worker证据为`runtime/ci/repair-workers-10.log/.xml`，单次109/109 PASS、零skip、269.98秒。focus14结果见下文，full14未启动，旧源结果不移用到a24f。

15阶段a24f的`runtime/build/frozen-image-source-proof-repair-09.json`确认两images09同源122py：worker `sha256:f9fdc00c73ce5cd0ff7316fc811d59cf3f292d07df25c156978959bbc74e6b9b`、Harness `sha256:b8da0b16efa26e5737790c56d7da14b3bcb55ffa17d7aa43b20eff5d7a3dd8f6`。core证据为`runtime/ci/repair-core-09.log/.xml`，1938 PASS/51 SKIP/31 DESELECTED/138.06秒；worker证据为`runtime/ci/repair-workers-11.log/.xml`，单次109/109 PASS、零skip、260.75秒。focus15已停止、full15未启动，不将该阶段结果移作下一源码验证。

16阶段bd74的`runtime/build/frozen-image-source-proof-repair-10.json`确认两images10同源122py：worker `sha256:6053072afd9ed4fabe618fb37f6798c56d842b847de0437dc1d587e4f4a08058`、Harness `sha256:ab2394b467f61400b624fd7988c9009b0d39d152958ce4ac6c52db472d339cde`。core证据为`runtime/ci/repair-core-10.log/.xml`，1984 PASS/51 SKIP/31 DESELECTED/144.26秒；worker证据为`runtime/ci/repair-workers-12.log/.xml`，单次109/109 PASS、零skip、264.21秒。focus16已停止/full16未启动，不移用到c97b。

17阶段c97b的`runtime/build/frozen-image-source-proof-repair-11.json`确认两images11同源122py：worker `sha256:9059b58f6724d29536917c6226a6dd0d539633dd53f895d7a186b5e111bcce1d`、Harness `sha256:55728000b792f2474512ae1b32e78be4af97ba74d75f342f3f40b75c771ec896`。core证据为`runtime/ci/repair-core-11.log/.xml`，1984 PASS/51 SKIP/31 DESELECTED/145.45秒；worker证据为`runtime/ci/repair-workers-13.log/.xml`，单次109/109 PASS、零skip、250.05秒。focus17为8/8 PASS、未选22 NOT_RUN，full17已停止，不借此证明下一源码。

18阶段6e4b的`runtime/build/frozen-image-source-proof-repair-12.json`确认两images12同源122py：worker `sha256:b3757737cdfc258b80c6c330a234cb8b84d33cd9aabb68ea3afc1b3a9861cfa2`、Harness `sha256:a6936e0458a32d0762b7d4779294f5141551587860e1616a70a877654c12448d`。core证据为`runtime/ci/repair-core-12.log/.xml`，2012 PASS/51 SKIP/31 DESELECTED/161.42秒；worker证据为`runtime/ci/repair-workers-14.log/.xml`，单次109 PASS、268.53秒。focus18有WEB01独立负审失败、full18从未启动，不移作下一源证明。

内部计量guard的临时source13 `cfd7177c…`与source14 `e8eaa9c8…`只用于付费前边界验证；后续收口否定、原样引用与翻译处理后改源，未启动19付费。这两阶段的冻结、core/镜像及worker记录保留，不把它们的计数转作当前ca7b结果。

19阶段ca7b的`runtime/build/frozen-image-source-proof-repair-15.json`确认两images15同源122py：worker `sha256:e5bd595b3384d7070c861f2230d4189203b0138f85a08ea12a6cc3a9872bfe92`、Harness `sha256:0a90a34fe448b89dafa625f5bb1be96f9246b2e6650b1966005bb87fc5dc9e82`。core证据为`runtime/ci/repair-core-15.log/.xml`，2060 PASS/51 SKIP/31 DESELECTED/164.53秒；worker证据为`runtime/ci/repair-workers-16.log/.xml`，109 PASS/零skip/267.75秒；不移用到4e36。

20阶段4e36的`runtime/build/frozen-image-source-proof-repair-16.json`确认两images16同源122py、uid65532：worker `sha256:be73178342d0729157531b797f2862e0138b9dcc526c0d053c52160179840fc4`、Harness `sha256:284099df55457dad1d40fb7a2ef8b5633495ccc505db94454d01d5973b38ca8a`。core证据为`runtime/ci/repair-core-16.log/.xml`，2068 PASS/51 SKIP/31 DESELECTED/184.72秒；worker17证据为`runtime/ci/repair-workers-17.log/.xml`，109 PASS/零skip/271.23秒；不移用到fae5。

21阶段fae5的`runtime/build/frozen-image-source-proof-repair-17.json`确认两images17同源122py、uid65532：worker `sha256:a0229382ca647cd210f447f4e4384fe3c5d55b38efd81d84e8a14fce84c9a72b`、Harness `sha256:0e255a5497973105b1dfe21235d2bd60c7fcc40f43ec03445eb064c9e8d4fd01`。core证据为`runtime/ci/repair-core-17.log/.xml`，2100 PASS/51 SKIP/31 DESELECTED/159.42秒；worker证据为`runtime/ci/repair-workers-18.log/.xml`，109 PASS/零skip/257.87秒，不移用到c411。

当前c411的`runtime/build/frozen-image-source-proof-repair-18.json`确认两images18同源122py、uid65532：worker `sha256:26ef64bd8c43ba0e1f71ce0a38611c31b26bcd025b8506b248017a463254ef26`、Harness `sha256:d62386859ebfb09d7b7656898b758da2d440a967ce1b8e2c0b029ae29f0cabc9`。core证据为`runtime/ci/repair-core-18.log/.xml`，2134 PASS/52 SKIP/31 DESELECTED/158.30秒；worker证据为`runtime/ci/repair-workers-19.log/.xml`，110 PASS/零skip/284.07秒。core多出的1项worker skip已在本轮实际worker覆盖，其余skip仍不算通过；schema001—012、SDK/runtime0.2.2rc2固定依赖层复用边界不变，不称全新下载或native独立重现。

候选包04是在文档验收仍待实测时构建的快照，不是最终docs/head包哈希。此前包02检查helper曾误将uv输出目录自动生成的.gitignore当archive，修正后仅重查原产物，确认未含.cache、runtime、.env等私有项，不是产品或包失败。最终文档冻结后的再次构建哈希由私有release manifest记录，避免候选包哈希冒充最终head或形成文档自引用。

source-freeze-01/02记录的gold文件字节SHA256均为`6c7c635a6a01a35f1cba9b471107aae8779ade5d2fe24af752ec0e5d4d19566c`，原case checker为`68d49fb934b682ffc6f383085cf4a74d5d628449ac566371bc35989cc63b3493`。gold与PASS阈值未改；文件字节hash与旧矩阵cases规范化hash`10f8ffbb…`分别记录。checker先以`0664a446…`修空comparison诊断，再以`fa0122bc35ace757beef4c2b78e3066e23390a335f5de9e4525f74dcb48e2aa5`修Mermaid别名作用域；各旧版本与失败保全，不能称checker全程未变。

## 最终本地验收

`runtime/live/runtime-matrix-22/summary-final.json`记录固定30项 **30 PASS、0 FAIL、0 NOT_RUN、0 NEEDS_REVIEW**，唯一产品源码为`c411860f470f9cb398177979185e9a8ea154275c571d07ecc38b7a5f6935cfc5`，`mixed_product_sources=false`，配置hash校验通过。汇总中的`provider_calls=0`指最后读取已保存证据的汇总阶段，真实执行本身调用了已授权API。

必要DOCX/WEB语义和Flow视觉复核均为本轮独立assistant审阅、非人工，未复用焦点或旧run。Flow原本地OCR漏掉Start、原FAIL保留；同一不可变原图经一次通用真实OCR读出Start/Review/Done后通过原有要求。没有修改原图、原文、gold或本轮checker。最终收口快照为`runtime/repair-20261115/acceptance22-final-snapshot.json`。

最后零付费汇总前后账本不增：有效上界 **17.266165 CNY**，4195笔settled、原5笔unknown/held0.562140；用户最新控制台累计实报仍为 **11.65 CNY**，后续调用按保守估算计入，原50/50 policy与revision1不变。真实微信、目标服务器容量、至少七天shadow及生产切换仍未运行，本轮未部署。

## 四项原失败与修复

| 原用例与失败 | 修复行为 | 当前可报告范围 |
| --- | --- | --- |
| RA-DOCX-02：provider_unavailable导致暂停 | live及Host纯模型步骤有界retry；每次独立费用预留、unknown保留，不重放已执行工具或不安全checkpoint | full21在fae5本轮review/检查PASS；当前c411完整22本轮PASS，b844故障注入及旧NR/失败保留 |
| RA-PDF-03：工作token预算耗尽 | 新建DOCX/PDF表单优先原生文档工具，不为该失败增加执行或金额预算 | full21在fae5实际PASS；12在947c亦PASS（4 round/37576 tokens/1 page），旧6暂停失败保留；当前c411完整22本轮PASS |
| RA-WEB-01：严格citation/access记录未满足 | canonical UTC、实际sources/所有额外断言审核与原请求交付合同；fresh文本研究不因router默认MD扩大权限 | fae5 focus21双WEB正式PASS，实模本次自行选text、强制MD纠正为PG证明；full21在WEB之前停止，当前c411 focus22取得必要检查/复核，完整22本轮PASS，旧失败/负审保留 |
| RA-SCHEDULE-03：通知将“检查合成实验清单”改写成“检查一下…” | 明确正文绑定原source；时间-only保留/正文替换重新提取，逐字校验和privacy清理；可信纯提醒空工具目录 | focus17在c97b实际触发completed/PASS；full19操作取消非新SUT断言FAIL，full20/21未运行到；当前c411完整22本轮PASS，真实微信仍NOT_RUN |

真实SDK故障注入走本地容器/native Harness路径，供应商故障为注入夹具，不是真实外部网络故障验收。该例在工具已写入一次后注入provider_unavailable，再进行纯模型重试；3个尝试使用独立预留，unknown不退款/不假结算，工具仅执行1次，新checkpoint安全。此证据不代表任意副作用都可安全重试。

提醒正文是数据：Schedule.goal保存明确原文，scope只保留来源绑定标记；触发时才产生Work.scope.reminder_body，避免在Schedule.scope留下额外正文副本。时间修改不能顺便改写正文；真实正文替换清理旧提示并绑定新源。独立执行criteria继续生效，正文中的“并帮我”等字样不授予额外工具权限。

## matrix07：局部结果与原生停止

matrix07使用b844；仅7条记录，原始结果为 **4 PASS、2 FAIL、1 NEEDS_REVIEW**。RA-DOCX-02后来取得独立assistant审阅通过，但原文件未改，不将原NEEDS_REVIEW偷偷改为PASS。

| 记录 | 保留的事实 |
| --- | --- |
| RA-PPTX-01 FAIL | 默认footer违反strict-only-content；critic又把pre_delivery尚未发送误作缺陷，原失败保留 |
| RA-PPTX-03 FAIL | 横向表数据与可编辑性正确，但未满足frozen row layout，严格失败保留 |
| 活动RA-PDF-02 | Root在改源前通过原生控制取消，不计为完成或通过 |
| 未启动项 | 22项未启动，不补作成功；07不是完整30项重跑通过 |

停止证明为`runtime/repair-20261115/matrix07-native-stop.json`，记录原生取消、仅中断本轮runner、原结果保留与budget_reset=false。没有直接覆写旧工作状态来获取通过记录。

d74a随后取消生成器的自动footer；通用name/value数据优先按每项一行表达，用户明确横向布局仍保留；critic明确当前处于pre_delivery阶段。它们修复生成/审阅行为，不改gold或放宽row-layout检查；08的实际结果见下节，提示约束未完全解决布局问题。

runner新增`--stop-on-failure`，遇首个FAIL停止后续付费任务；NEEDS_REVIEW不触发该停止。19项所属纯runner回归通过。这是验收执行控制，不改变用例评分或产品预算。

## matrix08：首FAIL自动停止

08在d74a记录前6项后自动止，原始结果为 **4 PASS、1 NEEDS_REVIEW（DOCX02）、1 FAIL（PPTX03）**。DOCX02已有独立assistant审核通过，仍与原NEEDS_REVIEW记录分列；PPTX01本轮真实通过。PPTX03仍为3个横向header加1行数字，数字和原生可编辑性正确，但严格row-layout检查不符，FAIL保留。

这说明通用提示尚未稳定约束工具数据布局。3896已改工具契约：默认records将明确单行纯数字键值向量规范化成两列行记录，用户显式columns保留横向；低层TableData和现有文件不自动转换。不硬编码此case、不改gold。31项本地字节/网关纯回归与28项真实PG合同已过；09在到达PPTX03前因另一PPTX01问题停止，不能据此宣布该case真实重验成功。

## matrix09：主题metadata与可见正文投影

09在3896的前4条后首FAIL自动止，原始结果为 **2 PASS、1 NEEDS_REVIEW（DOCX02，独立复核另列）、1 FAIL（PPTX01）**。PPTX01耗尽token预算，尚未进入artifact_deliver或semantic审核；不是records布局修复回退。

该次router将主题“合成项目启动”随机放入visible expected_text，而原文逐页指定不同标题并限制额外文字，导致Model反复把metadata主题补到页内。原router存储输出保留不改。321e已由隔离worker读回PPTX实际core-properties `metadata.title`；仅对fresh无输入、明确主题《X》且至少2个不同指定页标题、主题不重复且无展示/封面要求的请求，把主题要求从visible expected_text投影到metadata_title，验证实际metadata并给critic证据。后续编辑与显式可见标题不弱化；26项metadata纯回归和39项真实PG合同已过。10在PPTX前停止，尚无metadata修复的真实矩阵结论。

## matrix10：标题引用标记与坏响应暂停

10在321e记录前3项后首FAIL自动止，原始结果为 **1 PASS、1 NEEDS_REVIEW、1 FAIL（DOCX03 paused invalid_model_response）**。router.expected_text保留了《合成发布检查单》的引用定界符，生成的正确标题没有书名号，触发validation修复；第5次坏模型响应后暂停。metadata/PPTX尚未执行，不把该次停止归为metadata修复失败。

d492仅对fresh无输入的标题/题目/主题声明规范引用标记为实际标题字符；明确要求保留书名号、引号、逐字字面及重复引用的要求不弱化。metadata投影、可见要求和原router记录保持可审计。执行模型活动边界的有界retry扩展至invalid_model_response，未知费用留额、不重放工具。新增26项纯回归、56项真实PG工具合同和54项执行回归通过；core05为1893 PASS、51 SKIP、29 DESELECTED、123.60秒，images05已核对同源，worker07为98 PASS/零skip/240.29秒；11实际结果见下节。

## matrix11：原生表格证据与critic误读

11在d492的前6条后首FAIL自动止，原始结果为 **4 PASS、1 NEEDS_REVIEW（DOCX02，已独立复核另列）、1 FAIL（PPTX03 tokenbudget）**。DOCX03标题修复、PPTX01主题修复及PPTX02本轮实际PASS。

PPTX03已生成records原生表格；semantic critic把document_read聚合`unit.editable:false`误作Office表格不可编辑，反复要求改写并耗尽token。诊断证据为`runtime/repair-20261115/pptx03-run11-diagnosis.json`，该FAIL保留，不记作行布局回退。

947c已补实际Office bytes的字符串字段`metadata.native_table_count`：PPTX按形状has_table、DOCX按实际tables读回；明确`unit.editable`仅代表机器人文本locator能力。Runtime对明确原生/可编辑表格要求独立检查count>0，并将真实结构证据交给critic。原生结构纯37和真实PG工具37通过，core06为1913 PASS/51 SKIP/29 DESELECTED/152.49秒，静态通过；worker08为98 PASS/零skip/258.25秒，12实际结果见下节。

## matrix12：原生文档通过，图表OCR误读

12在947c的第16项CHART02首FAIL自动止，原始结果为 **14 PASS、1 NEEDS_REVIEW（DOCX02，已独立复核另列）、1 FAIL（CHART02）**。Word、PPTX、PDF、MD与Python/Shell、CHART01均已完成实际执行，其中DOCX02保持原NEEDS_REVIEW与独立复核分列，不偷偷改写原记录。

原PDF03本轮真实PASS，原生路径为4 round、37576 tokens、1 page；旧matrix06的10 round、145091 tokens后暂停仍保留。这一项实证不补作完整30项成功，也不移作后续源码重验结果。

CHART02图表spec、bytes与delivery均PASS，但`image_ocr_required_text`将W1读作WL而严格FAIL。另有已结算的独立DeepSeekOCR对相同像素只读出Title/Value；没有据此伪造通过。原图、本地OCR与独立观察均保留；纯sandbox盲试不同PSM/英中识别仍见Wi/WL。8f5a已改生成器的通用字体/标签可读性，不硬编码W1、不修改12的PNG、gold或checker；13的CHART02已在原checker自动OCR完全PASS，旧12停点已实际解决。

验收runner新增`--case-ids`依赖闭包焦点smoke，先覆盖原4失败及调度依赖，再运行完整新30；24项纯runner回归通过。gold不变，摘要仍以全30项为分母、未选项为NOT_RUN，恢复原run时不能更改已锁定的选择集合。焦点smoke不等于完整矩阵通过。

## focus13：原四失败与调度依赖6/6通过

新`runtime-focus-13`使用8f5a与images07，results revision1为 **6/6 PASS**。DOCX02与WEB01实际产物经独立assistant语义复核后零费用recheck；PDF03与Schedule03为原生exec PASS，Schedule01/02按设计reply通过。Root已读取结果确认。

此轮覆盖原四失败及其调度依赖，未选24项仍为NOT_RUN。完整`runtime-matrix-13`为同源新run，目前结果见下节；不将focus的6项或旧12的14项拼成完整30项通过。

## matrix13：最终29 PASS/1 FAIL，第30项输入准备暂停

完整13使用同8f5a与images07，在第17项FLOW01首FAIL止，原始结果为 **15 PASS、1 NEEDS_REVIEW（DOCX02独立审阅通过另列）、1 FAIL（FLOW01）**。前四格式12项、计算2项、图表2项已完成实际执行，通过检查或DOCX02独立审阅分列；CHART02本轮的标签已在原checker自动OCR完全PASS。

FLOW01的md edges、bytes、nonblank、validation与receipts均通过；原本地OCR只识别Start，缺失其他labels而严格FAIL。此后在同一immutable产物上追加真实OCR读出Start、Review、Done及独立visual review，原checker前17项取得17/17通过，再同源resume未开始的13项。原FAIL、local Start观察均保留，没有重放已完成Work，source8f5a未变。

最终只读汇总`runtime/live/runtime-matrix-13/summary-operator-stopped.json`为 **29 PASS、1 FAIL、0 NEEDS_REVIEW/30**，单源8f5a，含本轮4份独立assistant review与1次真实OCR侧录。原四失败、Schedule实际触发及EDIT01已PASS；不是30 PASS。原始失败、逐项记录与复核来源均保留。

第30项EDIT02在输入准备阶段paused invalid_attachment，没有checkpoint、events、bills或ToolReceipt，尚未进行模型执行。有效`input_versions`指向已验PPTX01原版，但同时把该原人类TEXT（attachments=[]）来源误放进`attachment_event_ids`；InputProcessor先处理空附件列表，尚未进入有效artifact导入便暂停，PPTX bytes并未损坏。293d只剔除经已有registry/ACL证明为所选artifact的空TEXT/QUOTE provenance冗余附件声明，仍保留fingerprints、撤销与真实malformed attachments fail-closed；新增12项PG与既有input 20项PG回归通过，不重放13的paused Work，新的真实验收待14。

runner checker对空comparison曾触发AttributeError，仍未把该项算PASS。Root已修正comparison/source/new/decoded为None或非dict时的诊断，使其稳定标记缺证据NOT_RUN；38项所属纯回归通过，gold与PASS阈值不变，不用诊断修复把缺结果算PASS。原checker `68d49f…`已归档为`runtime/repair-20261115/checker-before-empty-comparison.py`和`before.json`，新checker指纹为`0664a446…`；旧13失败、summary与证据不改。

## focus14：EDIT02通过，额外研究示例错误保留FAIL

293d的focus14 selected8最终为 **7 PASS、1 FAIL（WEB01）**；full14未启动。EDIT02源scope修复已实际completed/PASS，70.42秒，不重放旧13的paused Work。

WEB01本轮主体三定义、来源与canonical UTC正确，但未请求的示例`a=[5,2,3,1,4]`在两处排序结果漏5；独立负审`3365e211…`保留，不能判全过。a24f对text-only research且原文未请求code/example的情况，前置拒绝发布多余示例/fence，并通过原validation_failed有限修复；明确请求示例仍进入critic审核。执行指令与critic均要求输入/输出、元素与数值正确，不只核对主叙述。30项真实PG合同验证未请求时前置拦截、明确请求时仍到critic；core09与worker11通过仅归a24f，focus15未运行到WEB，不提前宣布真实复验通过。

## focus15：自包含逐页PPT被错误附件前置条件澄清

focus15 selected8在前2条后停止：**DOCX02 NEEDS_REVIEW、PPTX01 FAIL，后6项NOT_RUN**；full15未启动。PPTX01在执行前clarify，理由为`attachment_event_authorization`，要求没有附件的用户重发，同时又称无需附件即可。请求本身已自包含恰3页、逐页标题/文本且无图像需求；没有建Work，这不是文件生成失败。

bd74已在现有保守`new_work_intent`中补通用有限整串逐页PPT外层grammar，覆盖做成、恰N页、完整页序、第N页quoted标题正文及有限尾否定；主题不投作visible文字。未知或附加命令、引用、附件、修改、条件等仍走原Model；不硬编码用例、不越过source/role/quiet或最终门控。纯98/PG18通过，Ruff/format及intent的mypy通过；freeze10记录source为122py、gold`6c7c…`和checker`0664…`均保持该阶段版本不变。core10为1984 PASS/51 SKIP/31 DESELECTED/144.26秒、images10同源；worker12单次109/109 PASS、零skip、264.21秒仅归该阶段，focus16实际结果见下节。

## focus16：已绑定纯提醒被误作缺输入

bd74的focus16执行前7项后停止，固定30最终为 **6 PASS、1 FAIL（SCHEDULE03 waiting_input）、23 NOT_RUN、0 NEEDS_REVIEW**；full16未启动。本轮DOCX/WEB新独立review已PASS，WEB无多余示例。

Schedule03已有完整bound literal正文“检查合成实验清单”、origin rule、derived int1、active source/fingerprint且`required_tools=[]`；当时允许的唯一工具request_input让模型首轮把“提醒已生成…如需协助请提供清单”当required_input，进入waiting_input。该FAIL保留，不把正文/source正确当成通知已通过。

c97b对trusted literal reminder且无required_tools、无真实执行criteria时使用空catalog；非literal、真实执行criteria与普通缺输入仍走原工具路径。gateway按policy实际拒绝request_input（permission_denied），原来源、正文及投递独立校验保留。纯81及综合真实PG126 PASS；其中3个旧fixture的期望目录从request_input改为empty，断言未放宽。mypy122/Ruff通过，core11为1984 PASS/51 SKIP/31 DESELECTED/145.45秒、images11同源；worker13单次109/109 PASS、零skip、250.05秒、focus17为8/8 PASS、未选22 NOT_RUN，full17实际停止见下节。

## focus17：当前源码8/8通过，完整17另起新run

Root已读取c97b/images11的focus17结果：**selected8/8 PASS、0 FAIL、0 NEEDS_REVIEW**。本次DOCX与WEB独立语义review及零付费recheck均纳入，Schedule实际触发与EDIT02均completed/PASS。原四失败及EDIT02依赖在该源码上取得实际证明，不借旧focus16或其他源码结果。

此轮只有8项selected，未选22项仍NOT_RUN。完整`runtime-matrix-17`已以同c97b/images11另起新run，实际停止事实见下节；此前负review、paused与原OCR均保留，不能把focus成功写成完整30通过。

## matrix17：路由供应商不可用，第6项停止

full17在第6项PPTX03路由reply失败后停止。Root已读取实际decision，reason为`provider_unavailable`、confidence0；不是clarify、产物失败或表格布局回退。PPTX01/PPTX02本轮均实际completed/PASS，DOCX02本轮fresh review PASS。旧full17失败保留，完整30未通过；该次账本新增unknown及停止快照见费用表。

6e4b已为MeteredModel的路由/摘要落入显式有界pure-inference retry；默认retry limit=0保持兼容，app路由/摘要显式配置，Toolkit Reviewer使用min(current, original grant)。只对已经终止的纯model.complete所报provider_unavailable/invalid_model_response限次新预留重试，unknown留额；ledger/auth/permission/budget/rate_limited/cancel不重试，明确耗尽信号防止外层重试乘倍。原50元policy、gold和checker未提升或修改。

新增21项pure/5项PG，含账本域共46 PASS；Root共享review+metered 58项PG合同PASS，mypy122/Ruff通过。这些回归未调用真实API或改原50元账本，full17新增unknown的旧事实保留。freeze12为122py，core12为2012 PASS/51 SKIP/31 DESELECTED/161.42秒、mypy122/Ruff/format通过、images12同源；worker14单次109 PASS、268.53秒；focus18实际结果见下节，full18从未启动，不借旧结果。

## focus18：研究抓取计量单位错误，完整18从未启动

6e4b的focus18共8项执行完；WEB01独立复核发现额外数字单位错误：答复称本次读取12000字符，实际为11957 Unicode chars、12000 UTF-8 bytes。主体定义与UTC引用正确，仍严格判FAIL。零付费recheck最终`runtime/live/runtime-focus-18/summary-stopped-web01.json`为 **7 PASS、1 FAIL/selected8**，未选22项NOT_RUN；原NR与原答保留在revisions/0001，负审不改写。

零付费recheck前后账本均为3513 settled、5 unknown/held0.562140、有效上界36.515800 CNY，未新增费用。full18从未启动。其后的ca7b只修研究内部计量合同；原答、负审与gold保留，6e4b的core12/worker14不移用到后续版本。

## 19阶段ca7b：单项focus通过，完整矩阵停止

ca7b限制未请求的内部抓取计量表述，显式禁止、分别统计、跨句与混合单位边界均有定向回归。长数字匹配已采用有界处理；原样引用的明确quoted literal仅在分类副本中屏蔽，原答案保留，其他运行计量声明仍检查。显式翻译保守进入原semantic路径并携带完整真实metadata，负向critic仍拒绝虚假额外计量。38项pure与42项真实PG合同通过（含10新PG，原38 pure在PG运行中deselected，11.08秒）；独立4例复现通过只属定向边界证明，不称完整NLP泛化。

本轮`runtime-focus-19/summary-final`正式固定30为 **1 PASS、29 NOT_RUN**，只选WEB01。本次独立assistant语义review确认定义、来源类型及UTC正确，没有多余示例或计量错误，随后零付费recheck通过。未借之前8项或旧7个PASS；recheck前后states一致：3520 settled、5 unknown/held0.562140，上界36.661488 CNY，本焦点相对focus18增加0.145688保守估算，不是实际账单。

完整`runtime-matrix-19`已用同ca7b/images15新建、无过滤固定30、fail-fast启动。第17项FLOW01最初因本地PNG OCR为空及MD边检查而FAIL；实际MD含两条正确边，原checker`0664…`却把自然语言`Nodes: Start(top),Review(middle),Done(bottom)`误作Mermaid全局别名，得到top→middle→bottom。产品source、gold、MD及800×600 PNG均未改。

checker现为`fa0122bc35ace757beef4c2b78e3066e23390a335f5de9e4525f74dcb48e2aa5`；旧0664完整保存在`runtime/repair-20261115/checker-before-mermaid-scope.py`。`flow19-checker-scope-comparison.json`与独立17项old/new核对证明仅FLOW01的markdown_required_edges从FAIL变PASS，OCR仍硬FAIL；AST独立审查只见_markdown_edges变更。64项checker/runner/summary纯回归PASS（1.73秒，`runtime/ci/repair-checker-mermaid-scope.xml`），缺边、反向、跨graph/fence和非Mermaid代码等负例仍拒绝，未放宽gold或通过条件。

随后在同immutable PNG（e628…，input pixel de67c582…）追加一次真实OCR：仅genericimage.png、无prompt/gold，返回Start、Review、Done，DB settled receipt d5d5ecd3…；费用0.0015为保守估算。原localOCR空与原0664 raw FAIL完整保留在revisions/0001。正式recheck02实际 **17/17 PASS、0 FAIL/NEEDS_REVIEW**，前后states均3633 settled、5 unknown/held0.562140、有效上界39.253338 CNY不变，重检零付费。

随后同source/images/run仅`--resume`原未开始的13项，从IMAGE01继续，前17 Work无重放；续跑后WEB02独立语义复核失败，最终停止结果见下节。原始失败、负审、paused、原OCR与修订记录全部保留。

## 全19正式保全与当前4e36候选

零付费recheck04已记录28项为26 PASS、1 FAIL（WEB02）、1 NOT_RUN（中断SCHEDULE03），两项EDIT尚未开始。固定30的`summary-semantic-stop.json`按STOPPED规则最终为 **26 PASS、2 FAIL、2 NOT_RUN**；第二个FAIL明确为操作中断后native cancelled，不是新增调度SUT断言失败。Owner原生transition_work将ac290…从paused/worker_stopped rev5变为cancelled rev6，审计`matrix19-native-stop-control.json`及active=[]保全，没有直接改DB；原答、负审与逐项原始状态不改写。

WEB02的示例、ensure_ascii、实际fetch与UTC均正确，但额外称“若要写入文件需用UTF-8编码”，将推荐说成必需。官方receipt只要求fp.write接受str；本轮独立UTF-16 stdlib roundtrip对同一对象无损、32 bytes的反例纳入false review，FAIL保留。诊断发现真实页面11922 chars、输出13238，关键fp.write在页面位置4733却未进入旧critic整段JSON[:5000]，因此不能声称critic已看全证据。

4e36仅修research证据/事实合同：向critic传实际成功sources、去重、总计不超过12000 UTF-8 bytes，并明确标记省略；所有额外约束须有真实依据，推荐与必需区分，短答只答所问、概括而非复制长原文或添加未问的fileIO教程。不硬编码UTF-8、json或gold，不删旧答，不增加50元预算。42 pure及44真实PG合同共86 PASS仅是fixture合同回归，不声称实模效果；mypy122/Ruff/format281/Root diff通过，core16实际2068 PASS/51 SKIP/31 DESELECTED/184.72秒，worker17为109 PASS/零skip/271.23秒。

新focus20只选WEB01+WEB02，本轮独立非human review确认定义、所有旁注与实际fetch/UTC正确，WEB02的stdlib合成单键姓名输出吻合，没有19的UTF-8必需/print混淆，也未冒称全文；review与本次scope/answer SHA绑定。零付费正式复核为 **2/2 PASS**，固定30 summary为 **2 PASS、28 NOT_RUN**。重检前后states均3704 settled、5 unknown/held0.562140，上界40.630590 CNY完全不变；本焦点相对19停止快照增加0.316840保守估算，非实际账单。

随后另起同4e36/images16、无过滤完整30的`runtime-matrix-20`；checkerfa0122从起点固定、gold未改，使用本轮新回答/fresh review，不复用focus或旧ca7b的PASS。正式停止结果为 **20 PASS、1 FAIL、9 NOT_RUN、0 NEEDS_REVIEW**，`summary-stopped-web01.json`保全，active=[]且paid operator退出。

本轮第17项FLOW01唯一硬FAIL为原localOCR空，MD边/scopes、bytes、工具/格式、640×720尺寸与nonblank等均PASS。对同immutable 39ed… PNG、f76490… version、pixel3dff…进行本轮freshvisual review579b…实际PASS，再单次通用真实OCR读出Start、Review、Done，DB settled receipt1b3299cb…；原localOCR空与raw FAIL在revisions/0001保留。零付费recheck01实际17/17 PASS，前后3821 settled、5 unknown/held0.562140、上界43.419494 CNY不变；同source/images/checker/run仅续未始13，前17 Work无重放，未借19的OCR或focus通过数。

第21项WEB01随后safe=true暂停token_budget_exhausted：6 round、124118 tokens、6 tools、repairs1。原用户只要说明、链接与访问time，provider_candidate擅自给outputs md；首个文字候选因Missing requested formats被拒，修复进入workspace/artifact链后耗尽预算，未到semantic/交付，citation缺失是次生结果。旧答与失败不改写，不将该次失败说成引用/事实语义复核失败，也不增加token或50元预算。

## 21阶段fae5：原请求交付/权限合同

fae5以原请求约束fresh无输入文本research的交付，不能由router默认MD扩大为必须产文件；显式文件/报告、输入编辑和后续授权保留。Goal、criteria、outputs、工具目录、validator与rev2/audit保持一致，原router候选留作审计；独立复核确认PDF同句/语音音频的4项误挡已消除。不硬编码用例或提高token预算。新增32项（23 pure/9 PG）及相关256 mixed回归PASS（10.41秒），属于fixture合同而非实模效果；不称256 PG。

当前core17实际2100 PASS/51 SKIP/31 DESELECTED/159.42秒，worker18实际109 PASS/零skip/257.87秒，静态与两images17同源通过。唯一paid operator在开跑前核原50/50 policy、revision1、43.929850 CNY上界、3845 settled、5 unknown/held0.562140及active=[]，未扩预算。

新focus21本轮fresh 2 review及零付费复核正式 **2/2 PASS**，固定30为 **2 PASS、28 NOT_RUN**。首个实际WEB的canonical原文Goal、text outputs、criteria、message marker及required result合同一致；required web_search仍须真实成功，允许目录仅web_search/web_fetch/request_input，实际3 round/25798 tokens/2 tools，没有artifact链。本次实模planner本就选择text，不写成实模把错误MD纠正；强制MD纠正仅由PG合同证明。重检前后states及有效上界44.154286 CNY不变，本焦点增加0.224436保守估算，5 unknown/held0.562140保留，非实际账单。

随后新建同fae5/images17/checkerfa、无过滤固定30的full21，全部使用本轮新回答/fresh review，不复用focus。正式停止结果为 **15 PASS、1 FAIL、14 NOT_RUN**；CHART02原PNG数据与三次vision正确，但首critic把原PNG29b…与规范化view d67…的SHA直接比较，误拒后触发repair/token预算暂停，尚未交付。旧失败、原图/vision及46.821060 CNY旧guard快照保留。

## 当前c411：图像规范化绑定与Owner累计报告

c411在media/models/service/toolkit传递原SHA到规范化view SHA的真实映射，付费前校验实际payload并生成独立紧凑candidate proof；不能由模型自报映射来冒充原图身份，不硬编码CHART02或gold。媒体定向回归含8新PG、独立审查通过；新增真实worker用例已在worker19的110 PASS中覆盖。原图与规范化视图的字节SHA可以不同，不将原先正确数据/三vision误写为产品画图错误，也不把修复前未交付算PASS。

billing新增可信Owner显式opt-in累计报告模式：只替换已经settled的前缀保守估算，全部unknown/预留继续held，不分配未知实扣或伪settle；旧默认含unknown仍拒绝。billing105回归（18新）及旧V1审计roundtrip独立复核通过，旧schema1无新增字段；审计仍仅应用级追加、不是DB强制不可变，入口不暴露给模型/自然语言。

Root已零provider实际应用schema2审计282332、prev111620，用户累计报告11.650000 CNY替换旧settled-prefix估算后，有效上界12.212140=11.65+原held0.562140，3974 settled/5 unknown不变。policy/prices/reservations三金融表全量hash前后一致，原50/50 policy与revision1未变，旧46.821060上界和80.482936 gross估算保留；差额不是退款或新额度。

focus22三任务均completed：CHART02全11硬checks PASS（66.59秒），原生Work semantic accepted=true，4 round/56125 tokens/3 tools，chart_render→image_analyze→artifact_deliver，没有validation repair或重绘导出；真实candidate raw80587b96…与view1bcc9917…不同，新binding精确绑定file/version/raw而正确通过。双WEB completed为68.41/32.87秒，本轮fresh严格peer review均PASS。原raw1 PASS/2 NEEDS_REVIEW不改写；按用户只做必要验收的加速要求，不再串行统一focus recheck，不写正式3/3。

full22已同c411/images18/checkerfa无过滤完成固定30/30 PASS，所有全量回答/必要review均属本轮、不复用focus。唯一一次最终零付费汇总前后，4195笔settled与5笔unknown/held0.562140、有效上界17.266165均逐值不变；28个Work全部completed，另2项调度走reply，77个投递intent及77个part全部accepted。无活动Work、执行attempt或付费进程；本地接受回执不代表微信群手机端或成员已读。

## 费用与外部门槛

所有调用继续共用原`local-acceptance-20261114`账本，daily/total policy均50 CNY、revision1未变。没有新run额度、退款或清除unknown。

| 快照 | 有效预算用量上界 | 说明 |
| --- | --- | --- |
| 07开始前 | 21.023746 CNY | 此前验证轮快照，不是实际扣费 |
| 07停止后 | 22.516922 CNY | 含3笔unknown/held0.302090 CNY；未知结果继续保留 |
| 08首FAIL停止后 | 23.545822 CNY | gross settled57.467748为保守估算；unknown3笔/held0.302090保留，policy50/revision1未变；非实际扣费 |
| 09首FAIL停止后 | 24.303392 CNY | gross settled58.225318保守估算；unknown3/held0.302090，原policy50/revision1不变；非实际扣费 |
| 10首FAIL停止后 | 24.873772 CNY | gross settled58.648508保守估算；unknown4/held0.449280，原policy50/revision1不变；非实际扣费 |
| 11首FAIL停止后 | 26.014692 CNY | gross settled59.789428保守估算；unknown4/held0.449280，原policy50/revision1不变；非实际扣费 |
| 12首FAIL停止时 | 28.292702 CNY | gross settled62.067438保守估算；unknown4/held0.449280保留；额外独立OCR及后续费用不以本快照冒充最终总额；原policy50/revision1不变，非实际扣费 |
| 13在FLOW01停止时 | 31.327702 CNY | unknown4/held0.449280保留；原policy50/revision1不变，是历史实扣加保守suffix上界，非实际扣费或最终账本 |
| 13第30项输入暂停时 | 32.707920 CNY | unknown4/held0.449280保留；EDIT02尚未模型执行且无新增该项账单；原policy50/revision1不变，非实际扣费或最终账本 |
| focus14停止后 | 33.433298 CNY | unknown4/held0.449280保留；原policy50/revision1不变，非实际扣费或最终账本 |
| focus15停止后 | 33.609974 CNY | unknown4/held0.449280保留；原policy50/revision1不变，非实际扣费或最终账本 |
| focus16停止后 | 34.172378 CNY | unknown4/held0.449280保留；原policy50/revision1不变，非实际扣费或最终账本 |
| full17停止后 | 35.689242 CNY | 路由provider_unavailable新增1笔unknown预留0.112860；原4笔/0.449280完整保留，现unknown5/held0.562140。原policy50/revision1不变，非实际扣费 |
| focus18最终零付费recheck前后 | 36.515800 CNY | 两侧均3513 settled、5 unknown/held0.562140；recheck未新增费用。原policy50/revision1不变，非实际扣费 |
| focus19零付费recheck前后 | 36.661488 CNY | 两侧states均3520 settled、5 unknown/held0.562140；焦点调用增加0.145688保守估算，recheck零付费。原policy50/revision1未改，非实际扣费 |
| full19前17项recheck02前后 | 39.253338 CNY | 两侧均3633 settled、5 unknown/held0.562140；同PNG单次真实OCR增加0.0015保守估算，重检零付费，原50 policy/revision1未变，非实际扣费 |
| full19正式停止保全后 | 40.313750 CNY | recheck04前后3689 settled、5 unknown/held0.562140不变；原50 policy/revision1未改，非实际扣费 |
| focus20零付费复核前后 | 40.630590 CNY | 两侧均3704 settled、5 unknown/held0.562140不变；焦点调用增加0.316840保守估算、重检零付费；原50 policy/revision1未改，非实际账单 |
| full20前17项recheck01前后 | 43.419494 CNY | 两侧均3821 settled、5 unknown/held0.562140不变；本轮同PNG单次OCR增加0.0015保守估算，重检零付费，非实际账单 |
| full20正式停止保全后 | 43.929850 CNY | 3845 settled、5 unknown/held0.562140保留、active=[]；原50 policy/revision1未变，非实际账单 |
| focus21零付费正式复核前后 | 44.154286 CNY | states与上界前后不增；本焦点增加0.224436保守估算，5 unknown/held0.562140保留；原50 policy/revision1未变，非实际账单 |
| full21正式停止、对账前 | 46.821060 CNY | 旧guard口径；gross settled80.482936为保守估算，3974 settled/5 unknown的原记录保留，非实扣 |
| Owner新累计报告实际应用后 | 12.212140 CNY | schema2 audit282332/prev111620；用户报告累计11.650000+继续held0.562140，3974 settled/5 unknown不变；0 provider/三金融表hash不变，原50 policy/revision1未改，非退款/新额度 |
| focus22附近快照 | 12.644744 CNY | 用户累计11.65新基准+原held0.562140+新suffix保守估算；unknown5保留，非新实扣，不是最终全量账本 |
| full22最终收口 | 17.266165 CNY | 用户累计11.65+原unknown预留+后续保守suffix；4195 settled/5 unknown，唯一最后零付费汇总前后逐值不增，原50/revision1不变 |
| 当前完整22验证后 | **30/30 PASS；17.266165 CNY有效上界** | 同一c411源码与images18；最后零付费汇总前后账本不增，原50/revision1及5笔unknown保留 |

表中audit282332之前的历史上界按旧用户报告4.83 CNY前缀加后续保守估算/预留计算，旧数值与依据保留。用户最新明确报告控制台累计11.65 CNY并要求以此为准，累计金额不是4.83与11.65相加；来源仍为user-reported，不是connector独立验票。报告as_of采用Root收到“当前”答复的2026-11-15 06:21:31 UTC；用户未给精确查询时刻、期间paid/active已空，该时间限制已注明。

新证据为`runtime/billing/console-report-20261115-1165.json`，实际应用为`runtime/billing/console-reconciliation-20261115-1165.json`。Owner新累计报告即使包含未知调用已发生的实际费用，原5笔unknown也继续全额held以保守覆盖，不逐笔假结算。11.65是用户报告的累计实扣，12.212140是包含held的有效预算上界；之后新调用仍按保守估算/预留计入，同一50元总上限不重置、不退款、不增加额度。

真实微信L5、目标Linux容量、至少七天shadow、真实数据迁移及单发送者切换全部 **NOT_RUN**。当前不部署；完整矩阵和最终CI结果取得后，仍不能据本地证据宣称所有A01—A48或生产验收通过。

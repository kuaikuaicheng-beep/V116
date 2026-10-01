import os, re, sqlite3, asyncio, json, io, traceback, random
from datetime import datetime, timezone
import discord
from discord import app_commands
from discord.ext import commands, tasks


TOKEN = os.getenv('DISCORD_TOKEN','').strip()
DB_PATH = os.getenv('DB_PATH','/data/trade_bot.db' if os.path.isdir('/data') else 'trade_bot.db')

PRODUCTS=['50M','100M']
COIN_PRODUCTS=['50M 有33等','100M 無33等','100M 有33等','不死號（不會被官方掃幣號封）']
BOOST_TIERS=[f'{x}M' for x in [50,100,150,200,300,400,500,600,700,800,900,1000]]
# V44：/查詢工單 的商品 choices 必須在 decorator 載入前存在。
QUERY_PRODUCT_CHOICES = COIN_PRODUCTS + BOOST_TIERS
STATUSES=['結單','待付款','待確認付款','待洽談','待排單','待交貨','處理中','待收貨','有爭議','待處理','已取消']
AVAILABILITY=['正常提供','暫停提供','缺貨']
# 只把「看起來像工單」的頻道視為新工單，避免一般數字頻道被誤判。
TICKET_STATUSES = STATUSES
RENAME_ONLY_STATUSES = ('待倒',)
TICKET_NAME_STATUSES = tuple(dict.fromkeys((*TICKET_STATUSES, *RENAME_ONLY_STATUSES)))
TICKET_STATUS_PATTERN = '|'.join(re.escape(x) for x in TICKET_NAME_STATUSES)
CLOSED_MARKERS=('結單','已結單','已關閉','closed','close','archived','archive')

db=sqlite3.connect(DB_PATH,check_same_thread=False,timeout=15)
db.row_factory=sqlite3.Row
db.execute('PRAGMA journal_mode=WAL')
db.execute('PRAGMA synchronous=NORMAL')
db.execute('PRAGMA busy_timeout=15000')
db.execute('PRAGMA foreign_keys=ON')
lock=asyncio.Lock()

def q(sql,params=(),fetch=False):
    # SELECT / PRAGMA 不需要 commit，避免每一則訊息都產生不必要的磁碟 I/O。
    c=db.cursor(); c.execute(sql,params); rows=c.fetchall() if fetch else None
    first=(sql.lstrip().split(None,1)[0].upper() if sql.lstrip() else '')
    if first not in ('SELECT','PRAGMA','EXPLAIN'):
        db.commit()
    return rows

def now(): return datetime.now(timezone.utc).isoformat()

def key(guild_id,k): return f'g:{guild_id}:{k}'
def get(guild_id,k,default=''):
    r=q('SELECT value FROM settings WHERE key=?',(key(guild_id,k),),True); return r[0]['value'] if r else default
def setv(guild_id,k,v): q('INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',(key(guild_id,k),str(v)))

def choices(vals): return [app_commands.Choice(name=x,value=x) for x in vals]

def init_db():
    q('''CREATE TABLE IF NOT EXISTS orders(id INTEGER PRIMARY KEY AUTOINCREMENT,ticket_no TEXT NOT NULL,channel_id TEXT NOT NULL,guild_id TEXT NOT NULL,product TEXT NOT NULL,quantity INTEGER NOT NULL,unit_price INTEGER NOT NULL,total_price INTEGER NOT NULL,status TEXT NOT NULL DEFAULT '待付款',buyer_id TEXT,created_at TEXT NOT NULL,paid_at TEXT,completed_at TEXT,delivery_time TEXT,payment_method TEXT,payment_reminder_sent INTEGER NOT NULL DEFAULT 0)''')
    # 舊版資料庫相容：補上新流程需要的欄位。
    cols={r['name'] for r in q('PRAGMA table_info(orders)',(),True)}
    if 'delivery_time' not in cols: q('ALTER TABLE orders ADD COLUMN delivery_time TEXT')
    if 'payment_method' not in cols: q('ALTER TABLE orders ADD COLUMN payment_method TEXT')
    if 'payment_reminder_sent' not in cols: q('ALTER TABLE orders ADD COLUMN payment_reminder_sent INTEGER NOT NULL DEFAULT 0')
    if 'service_type' not in cols: q("ALTER TABLE orders ADD COLUMN service_type TEXT NOT NULL DEFAULT '幣號'")
    if 'game_account' not in cols: q('ALTER TABLE orders ADD COLUMN game_account TEXT')
    if 'payment_proof_url' not in cols: q('ALTER TABLE orders ADD COLUMN payment_proof_url TEXT')
    if 'payment_proof_message_id' not in cols: q('ALTER TABLE orders ADD COLUMN payment_proof_message_id TEXT')
    if 'stock_reserved' not in cols: q('ALTER TABLE orders ADD COLUMN stock_reserved INTEGER NOT NULL DEFAULT 0')
    q('''CREATE TABLE IF NOT EXISTS products(product TEXT PRIMARY KEY,price INTEGER NOT NULL DEFAULT 0,stock INTEGER NOT NULL DEFAULT 0,enabled INTEGER NOT NULL DEFAULT 1,availability_status TEXT NOT NULL DEFAULT '正常提供')''')
    pcols={r['name'] for r in q('PRAGMA table_info(products)',(),True)}
    if 'availability_status' not in pcols: q("ALTER TABLE products ADD COLUMN availability_status TEXT NOT NULL DEFAULT '正常提供'")
    q('''CREATE TABLE IF NOT EXISTS boost_prices(guild_id TEXT NOT NULL,tier TEXT NOT NULL,price INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(guild_id,tier))''')
    q('''CREATE TABLE IF NOT EXISTS ticket_channels(channel_id TEXT PRIMARY KEY,guild_id TEXT NOT NULL,ticket_no TEXT NOT NULL,first_name TEXT NOT NULL,buyer_id TEXT,buyer_name TEXT,detected_at TEXT NOT NULL)''')
    q('''CREATE TABLE IF NOT EXISTS ticket_counters(guild_id TEXT PRIMARY KEY,next_no INTEGER NOT NULL DEFAULT 1)''')
    q('''CREATE TABLE IF NOT EXISTS balances(user_id TEXT PRIMARY KEY,balance INTEGER NOT NULL DEFAULT 0)''')
    q('''CREATE TABLE IF NOT EXISTS balance_logs(id INTEGER PRIMARY KEY AUTOINCREMENT,guild_id TEXT NOT NULL,user_id TEXT NOT NULL,operator_id TEXT NOT NULL,amount INTEGER NOT NULL,balance_after INTEGER NOT NULL,action TEXT NOT NULL,created_at TEXT NOT NULL,note TEXT)''')
    q('''CREATE TABLE IF NOT EXISTS order_logs(id INTEGER PRIMARY KEY AUTOINCREMENT,order_id INTEGER NOT NULL,guild_id TEXT NOT NULL,operator_id TEXT,action TEXT NOT NULL,detail TEXT,created_at TEXT NOT NULL)''')
    q('''CREATE TABLE IF NOT EXISTS status_posts(order_id INTEGER PRIMARY KEY,channel_id TEXT NOT NULL,message_id TEXT NOT NULL,updated_at TEXT NOT NULL)''')
    q('''CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT NOT NULL DEFAULT '')''')
    q('CREATE INDEX IF NOT EXISTS idx_orders_channel_buyer_status ON orders(channel_id,buyer_id,status)')
    q('CREATE INDEX IF NOT EXISTS idx_orders_guild_status_reminder ON orders(guild_id,status,payment_reminder_sent)')
    q('CREATE INDEX IF NOT EXISTS idx_orders_guild_created ON orders(guild_id,created_at)')
    q('CREATE INDEX IF NOT EXISTS idx_ticket_channels_guild_buyer ON ticket_channels(guild_id,buyer_id)')
    q('CREATE INDEX IF NOT EXISTS idx_ticket_channels_guild_no ON ticket_channels(guild_id,ticket_no)')
    for p in PRODUCTS: q('INSERT OR IGNORE INTO products(product,price,stock,enabled) VALUES(?,?,?,1)',(p,0,0))
init_db()
for _p in COIN_PRODUCTS:
    q('INSERT OR IGNORE INTO products(product,price,stock,enabled,availability_status) VALUES(?,?,?,?,?)',(_p,0,0,1,'正常提供'))

intents=discord.Intents.default(); intents.guilds=True; intents.message_content=True
bot=commands.Bot(command_prefix='!',intents=intents)
# 已知工單頻道快取：on_message 不再每一則普通聊天都查 SQLite。
TICKET_CHANNEL_IDS=set()
TICKET_SYNCED=False
STATUS_POST_LOCKS = {}
AUTO_CATEGORY_LOCKS = {}
RENUMBER_LOCK = asyncio.Lock()
AUTO_CATEGORY_MODES=['工單號小-大','工單號大-小','按照目前排序狀況','隨機']
# 防止同一位客人快速連點「開始開單」造成重複工單。
OPEN_TICKET_LOCKS = {}
PANEL_SEND_LOCKS = {}
VIEWS_REGISTERED = False
OPEN_PANEL_LOCKS = {}
DETECTION_LOCKS = {}

def admin(m):
    """統一判斷 CT 小舖管理員。

    以 Discord 實際 Member 權限優先，再檢查設定的管理員身分組。
    不依賴快取中的名稱單一判斷，避免角色改名／重啟後偶發被判定為沒有權限。
    """
    if not isinstance(m,discord.Member): return False
    try:
        perms=m.guild_permissions
        if perms.administrator or perms.manage_guild or perms.manage_channels:
            return True
    except Exception:
        pass
    try:
        rid=get(m.guild.id,'admin_role_id','')
        if rid and any(str(r.id)==str(rid) for r in m.roles):
            return True
        # 相容既有 CT 小舖常用管理角色名稱；ID 設定仍然是最可靠方式。
        admin_names={'管理員','店長','店長👑','闆闆👑'}
        return any((r.name or '').strip() in admin_names for r in m.roles)
    except Exception as e:
        print(f'[ADMIN_CHECK] guild={getattr(getattr(m,"guild",None),"id",None)} user={getattr(m,"id",None)} error={e!r}')
        return False

def ticket_no(name):
    """嚴格辨識工單號。

    規則優先順序：
    1. 明確的「狀態-服務-工單號-額度」格式
    2. 舊版「狀態-工單號-額度/數量」格式
    3. closed/close/archived + 工單號
    4. ticket/工單 + 工單號
    5. 最後才接受「狀態/服務 + 分隔符 + 數字」格式

    重點：不會把 50M、100M、200M 或數量誤認成工單號。
    """
    name = (name or '').strip()
    if not name:
        return None

    # Discord 頻道名稱會把部分字元正規化；統一比較用的分隔符。
    n = re.sub(r'\s+', ' ', name)

    # ① 現行改名工單格式：
    #    待倒-代肝-0027-100m
    #    結單-幣號-0042-50m
    #    結單-幣號-0178-100m1隻
    # 工單號「必須」位於服務名稱後面的獨立數字段。
    m = re.fullmatch(
        rf'(?:{TICKET_STATUS_PATTERN})[-_ ]+(?:幣號|代肝)[-_ ]+(\d{{1,8}})(?:[-_ ].*)?',
        n, re.I
    )
    if m:
        return m.group(1)

    # ② ticket + 工單號：
    #    ticket-0018-c'
    #    ticket-0018
    #    ticket-幣號-0018-100m
    #    ticket_support_0018-extra
    m = re.match(r'(?i)^ticket(?:[-_ ]+(?:幣號|代肝))?[-_ ]+(\d{1,8})(?:[-_ ].*)?$', n)
    if m:
        return m.group(1)

    # ticket 後面有額外文字時，仍只抓「ticket 後第一個合理數字」，
    # 但禁止從額度 50M/100M 取號。
    if re.match(r'(?i)^ticket(?:[-_ ]|$)', n):
        nums = list(re.finditer(r'(?<!\d)(\d{1,8})(?!\d)', n))
        for x in nums:
            pos = x.start(1)
            after = n[pos + len(x.group(1)):pos + len(x.group(1)) + 2]
            if not re.match(r'(?i)M', after):
                return x.group(1)

    # ③ closed / close / archived：
    #    closed-0042
    #    closed-幣號-0042-50m
    m = re.match(
        r'(?i)^(?:closed|close|archived|archive)(?:[-_ ]+(?:幣號|代肝))?[-_ ]+(\d{1,8})(?:[-_ ].*)?$',
        n
    )
    if m:
        return m.group(1)

    # ④ 「工單」關鍵字：
    #    工單-0018
    #    工單-幣號-0018-100m
    #    工單 #0018
    m = re.match(rf'^(?:工單)(?:[-_ ]+(?:幣號|代肝))?[-_ #：:]+(\d{{1,8}})(?:[-_ ].*)?$', n, re.I)
    if m:
        return m.group(1)

    if '工單' in n:
        nums = list(re.finditer(r'(?<!\d)(\d{1,8})(?!\d)', n))
        for x in nums:
            pos = x.start(1)
            before = n[max(0, pos - 3):pos]
            after = n[pos + len(x.group(1)):pos + len(x.group(1)) + 3]
            # 額度／數量組合，例如「工單-50M2隻」：50 是額度、2 是數量，兩者都不能當工單號。
            if re.search(r'(?i)m\s*$', before) or re.match(r'(?i)^m', after):
                continue
            if re.search(r'(?i)\d+\s*m\s*$', before) and re.match(r'\s*隻', after):
                continue
            if '#' in before or re.match(r'(?i)^[-_ ]', before[-1:] or ' ') or not re.match(r'(?i)m', after):
                # 數字若直接是「數量＋隻」且前方已有 M，排除。
                if re.search(r'(?i)m\s*$', before):
                    continue
                return x.group(1)

    # ⑤ 舊版：狀態-工單號-額度/數量
    #    結單-0042-50m
    #    結單-0178-100m1隻
    m = re.fullmatch(
        rf'(?:{TICKET_STATUS_PATTERN})[-_ ]+(\d{{1,8}})[-_ ]+.*',
        n, re.I
    )
    if m:
        return m.group(1)

    # ⑥ 最後保底：狀態/服務 + 分隔符 + 數字。
    # 只接受「狀態或服務後面第一個獨立數字段」，不掃整串找最後一個數字。
    if n.startswith(tuple(TICKET_STATUSES)) or '幣號' in n or '代肝' in n:
        m = re.search(r'(?:' + TICKET_STATUS_PATTERN + r'|幣號|代肝)[-_ ]+(\d{1,8})(?!\d)', n, re.I)
        if m:
            candidate = m.group(1)
            after = n[m.end(1):m.end(1)+2]
            # 若這個數字直接是 50M/100M 等額度，不能當工單號。
            if not re.match(r'(?i)M', after):
                return candidate

    return None

def looks_closed(name):
    n=(name or '').lower()
    return any(marker.lower() in n for marker in CLOSED_MARKERS)

def is_ticket_candidate(ch):
    if not isinstance(ch,discord.TextChannel): return False
    name=(ch.name or '').strip()
    if q('SELECT 1 FROM ticket_channels WHERE channel_id=? LIMIT 1',(str(ch.id),),True): return True
    if q('SELECT 1 FROM orders WHERE channel_id=? LIMIT 1',(str(ch.id),),True): return True
    # 新建外部工單、已改名工單、舊版狀態工單都可辨識；一般頻道必須有明確工單語意。
    return bool(ticket_no(name)) and (
        bool(re.match(r'(?i)^ticket', name)) or '工單' in name or
        any(name.startswith(st) for st in TICKET_STATUSES) or '幣號' in name or '代肝' in name
    )

def cn(n):
    if 1<=n<=10: return ['', '一','二','三','四','五','六','七','八','九','十'][n]
    return str(n)

def price(p):
    r=q('SELECT price FROM products WHERE product=? AND enabled=1',(p,),True); return int(r[0]['price']) if r else 0

def stock(p):
    r=q('SELECT stock FROM products WHERE product=?',(p,),True); return int(r[0]['stock']) if r else 0

def availability(p):
    r=q('SELECT availability_status FROM products WHERE product=?',(p,),True); return r[0]['availability_status'] if r else '正常提供'

def boost_price(gid,tier):
    r=q('SELECT price FROM boost_prices WHERE guild_id=? AND tier=?',(str(gid),tier),True); return int(r[0]['price']) if r else 0

# ---- 聊天室自動算價 V28 ----
# V84：聊天室自動算價不把 50M／100M／任何帶 M 的額度當成新的數字輸入。
CALC_ITEM_RE = re.compile(r'(?:幣號\s*)?(?:代肝\s*)?(\d{1,5})(?!\s*[mM]\b)(?:\s*(?:\*|[xX×])\s*(\d{1,5}))?')

def _calc_coin_unit(guild, amount):
    # V75：聊天室自動算價不再寫死只有 50M／100M。
    # 只要產品資料庫有「150M、200M、300M…」等額度，就能使用同一格式試算。
    amount=int(amount)
    if amount <= 0:
        return None
    product=f'{amount}M'
    rows=q('SELECT price FROM products WHERE product=? AND enabled=1',(product,),True)
    if rows and int(rows[0]['price'])>0:
        return int(rows[0]['price'])
    # 相容「100M 有33等」等同額度多規格商品；只有全部啟用規格單價一致時才自動採用，
    # 避免多個不同價格規格時算錯。
    rows=q('SELECT price FROM products WHERE product LIKE ? AND enabled=1 ORDER BY product',(product+'%',),True)
    prices=[int(r['price']) for r in rows if int(r['price'])>0]
    return prices[0] if prices and len(set(prices))==1 else None

def _calc_boost_unit(guild, amount):
    tier=f'{int(amount)}M'
    if tier not in BOOST_TIERS:
        return None
    price=boost_price(guild.id,tier)
    return price if price>0 else None

def _normalize_calc_text(text):
    """把聊天室常見的數量寫法統一成「額度*數量」。"""
    t=(text or '').strip()
    # 中文數字數量：一～十、兩、幾個常見寫法
    cn={'一':1,'兩':2,'二':2,'三':3,'四':4,'五':5,'六':6,'七':7,'八':8,'九':9,'十':10}
    for k,v in sorted(cn.items(), key=lambda x:-len(x[0])):
        t=re.sub(rf'(?i)(\d{{1,5}})\s*[mM]?\s*{k}\s*(?:隻|支|個)?', rf'\1M*{v}', t)
    # 數字數量：50m2隻 / 50m 2隻 / 50*2隻 / 50m*2支
    t=re.sub(r'(?i)(\d{1,5})\s*[mM]?\s*(?:\*|[xX×])\s*(\d{1,5})\s*(?:隻|支|個)', r'\1M*\2', t)
    # 沒有乘號但直接寫「50m 2隻」也支援。
    t=re.sub(r'(?i)(\d{1,5})\s*[mM]\s*(\d{1,5})\s*(?:隻|支|個)', r'\1M*\2', t)
    return t

def parse_price_calculation(guild, content):
    text=_normalize_calc_text((content or '').strip())
    if not text or len(text)>200:
        return None
    is_boost='代肝' in text
    is_coin='幣號' in text
    # 沒有寫「代肝／幣號」時，預設視為目前店內的幣號試算。
    if is_boost and is_coin:
        return None
    body=re.sub(r'代肝|幣號','',text).strip()
    # V84：自動算價只接受「純數字」額度；像 50M、100M、200M 這種已帶 M 的文字不偵測。
    token=r'\d{1,5}(?!\s*[mM]\b)\s*(?:\s*(?:\*|[xX×])\s*\d{1,5})?'
    full=re.fullmatch(rf'\s*{token}(?:\s*(?:\+|,|，|、|\s)\s*{token})*\s*',body)
    if not full:
        return None
    matches=list(re.finditer(token,body))
    if not matches:
        return None
    total=0; details=[]
    for m in matches:
        raw=m.group(0).strip()
        am=re.match(r'(\d{1,5})',raw)
        if not am: return None
        amount=int(am.group(1))
        qm=re.search(r'(?:\*|[xX×])\s*(\d{1,5})\s*$',raw)
        qty=int(qm.group(1)) if qm else 1
        if qty<=0 or qty>10000: return None
        unit=_calc_boost_unit(guild,amount) if is_boost else _calc_coin_unit(guild,amount)
        if unit is None:
            service='代肝' if is_boost else '幣號'
            return {'type':'missing','text':f'❌ 找不到 {amount}M 的{service}價格。'}
        subtotal=unit*qty; total+=subtotal
        details.append(f'{amount}M × {qty} = {subtotal:,} NT')
    service='代肝' if is_boost else '幣號'
    return {'type':'ok','text':f'💰 **{service}價格試算**\n'+'\n'.join(details)+f'\n\n**總計：{total:,} NT**'}


def channel_link(guild,name):
    setting='coin_price_channel_id' if name=='幣號價目表' else 'boost_price_channel_id'
    cid=get(guild.id,setting,'')
    ch=guild.get_channel(int(cid)) if cid.isdigit() else None
    if not isinstance(ch,discord.TextChannel): ch=discord.utils.get(guild.text_channels,name=name)
    return ch.mention if ch else f'#{name}'

def off_hours(gid):
    return get(gid,'off_hours','0')=='1'

def ticket_record(ch):
    r=q('SELECT * FROM ticket_channels WHERE channel_id=?',(str(ch.id),),True); return r[0] if r else None

def remember(ch,no,buyer_id=None,buyer_name=None):
    TICKET_CHANNEL_IDS.add(str(ch.id))
    r=ticket_record(ch)
    if r:
        # 名稱／目前辨識結果優先；不要讓舊快取覆蓋剛整理出的正確工單號。
        updates=[]; params=[]
        if no is not None and str(no).isdigit() and str(r['ticket_no'] or '') != str(no):
            updates.append('ticket_no=?'); params.append(str(no))
        if buyer_id is not None and str(buyer_id).isdigit() and str(r['buyer_id'] or '') != str(buyer_id):
            updates.append('buyer_id=?'); params.append(str(buyer_id))
        if buyer_name and str(r['buyer_name'] or '') != str(buyer_name):
            updates.append('buyer_name=?'); params.append(str(buyer_name))
        if updates:
            params.append(str(ch.id)); q(f"UPDATE ticket_channels SET {', '.join(updates)} WHERE channel_id=?",tuple(params))
        return str(no) if no is not None else r['ticket_no']
    q('INSERT INTO ticket_channels(channel_id,guild_id,ticket_no,first_name,buyer_id,buyer_name,detected_at) VALUES(?,?,?,?,?,?,?)',(str(ch.id),str(ch.guild.id),str(no),ch.name,str(buyer_id) if buyer_id else None,buyer_name,now()))
    return no

def _is_staff_member(member):
    if not isinstance(member, discord.Member):
        return True
    if member.bot:
        return True
    if member.guild_permissions.administrator:
        return True
    # 以「管理員」名稱及已設定的管理員身分組雙重排除，避免把店長／管理員當成客人。
    admin_role_id = get(member.guild.id, 'admin_role_id', '')
    if admin_role_id and any(str(r.id) == str(admin_role_id) for r in member.roles):
        return True
    if any(r.name == '管理員' for r in member.roles):
        return True
    return False


def _member_can_view_channel(ch, member):
    try:
        return ch.permissions_for(member).view_channel
    except Exception:
        return False


async def detect_buyer(ch):
    """盡可能從「頻道本身」找出客人，而不是只依賴資料庫。

    判斷優先級：
    1. 頻道對「會員本人」的明確權限覆寫。
    2. 頻道實際可見的會員（在快取可用時）。
    3. 具有闆闆👑角色且可看頻道的會員。

    永遠排除 Bot、Administrator、管理員角色及設定的管理員角色。
    """
    candidates = []
    seen = set()

    # 第一層：最可靠——頻道對特定使用者的 Permission Overwrite。
    for target, overwrite in ch.overwrites.items():
        if not isinstance(target, discord.Member) or _is_staff_member(target):
            continue
        # Ticket Bot 通常會直接給客人 view_channel=True。
        # 即使沒有明確寫 True，也用實際權限再確認一次，兼容不同 Ticket Bot 的關閉方式。
        explicit_view = overwrite.view_channel is True
        actual_view = _member_can_view_channel(ch, target)
        if explicit_view or actual_view:
            if target.id not in seen:
                candidates.append(target)
                seen.add(target.id)

    if candidates:
        # 若有多個一般會員覆寫，優先闆闆👑角色；否則取第一個明確客人覆寫。
        candidates.sort(key=lambda m: (not any(r.name == '闆闆👑' for r in m.roles), m.id))
        m = candidates[0]
        return m.id, m.display_name

    # 第二層：discord.py 已有成員快取時，直接看「這個頻道誰看得到」。
    # 不要求闆闆👑角色，因為關閉工單後 Ticket Bot 可能會改角色／權限。
    try:
        for member in getattr(ch, 'members', []):
            if _is_staff_member(member):
                continue
            if _member_can_view_channel(ch, member) and member.id not in seen:
                candidates.append(member)
                seen.add(member.id)
    except Exception:
        pass

    if candidates:
        candidates.sort(key=lambda m: (not any(r.name == '闆闆👑' for r in m.roles), m.id))
        m = candidates[0]
        return m.id, m.display_name

    # 第三層：兼容舊 Ticket Bot——如果頻道仍保留闆闆👑會員權限，就使用它。
    try:
        for target, overwrite in ch.overwrites.items():
            if not isinstance(target, discord.Member) or _is_staff_member(target):
                continue
            if not any(r.name == '闆闆👑' for r in target.roles):
                continue
            if overwrite.view_channel is not False and target.id not in seen:
                return target.id, target.display_name
    except Exception:
        pass

    # 第四層：關閉工單最可靠的備援——直接讀取頻道歷史訊息。
    # 很多 Ticket Bot 關單時會刪掉客人的 Permission Overwrite，因此「頻道裡誰有權限」
    # 會失效；但客人曾經在這張工單發過訊息，Discord 的訊息作者仍然存在。
    # 以最近 200 則訊息找「非 Bot、非管理員、非管理員角色」的真人，
    # 優先取最早出現的非管理人員，避免把店長後續回覆誤判成客人。
    try:
        history_candidates = []
        history_seen = set()
        async for msg in ch.history(limit=200, oldest_first=True):
            author = getattr(msg, 'author', None)
            if not isinstance(author, discord.Member) or _is_staff_member(author):
                continue
            if author.id in history_seen:
                continue
            history_seen.add(author.id)
            history_candidates.append(author)
        if history_candidates:
            # 若其中有人有闆闆👑，優先；否則取最早發言的真人。
            history_candidates.sort(key=lambda m: (not any(r.name == '闆闆👑' for r in m.roles)))
            m = history_candidates[0]
            return m.id, m.display_name
    except Exception:
        pass

    return None, None

async def buyer_for(ch):
    """取得工單客人；已關閉工單仍盡量保留原客人。"""
    r = ticket_record(ch)

    # 1. 工單建立時已保存的 buyer_id 是第一優先。
    if r and r['buyer_id']:
        try:
            bid = int(r['buyer_id'])
        except (TypeError, ValueError):
            bid = None
        if bid:
            member = ch.guild.get_member(bid)
            bname = (member.display_name if member else r['buyer_name']) or '客人'
            if bname != (r['buyer_name'] or ''):
                q('UPDATE ticket_channels SET buyer_name=? WHERE channel_id=?', (bname, str(ch.id)))
            return bid, bname

    # 2. 從該工單最後一筆訂單記錄補回 buyer_id。
    rows = q('SELECT buyer_id FROM orders WHERE channel_id=? AND buyer_id IS NOT NULL ORDER BY id DESC LIMIT 1', (str(ch.id),), True)
    if rows and rows[0]['buyer_id']:
        try:
            bid = int(rows[0]['buyer_id'])
        except (TypeError, ValueError):
            bid = None
        if bid:
            member = ch.guild.get_member(bid)
            bname = member.display_name if member else (r['buyer_name'] if r else None)
            if r:
                q('UPDATE ticket_channels SET buyer_id=?,buyer_name=COALESCE(?,buyer_name) WHERE channel_id=?', (str(bid), bname, str(ch.id)))
            else:
                q('INSERT OR IGNORE INTO ticket_channels(channel_id,guild_id,ticket_no,first_name,buyer_id,buyer_name,detected_at) VALUES(?,?,?,?,?,?,?)', (str(ch.id), str(ch.guild.id), ticket_no(ch.name) or '0', ch.name, str(bid), bname, now()))
            return bid, bname

    # 3. 直接從「目前頻道」找客人。這是關閉工單最重要的備援。
    bid, bname = await detect_buyer(ch)
    if bid:
        if r:
            q('UPDATE ticket_channels SET buyer_id=?,buyer_name=? WHERE channel_id=?', (str(bid), bname, str(ch.id)))
        else:
            no = ticket_no(ch.name)
            if no:
                remember(ch, no, bid, bname)
        return bid, bname

    # 4. 如果曾經保存過名稱，即使現在找不到 Member，也保留原名稱，不回退成「客人」。
    if r and r['buyer_name']:
        return (int(r['buyer_id']) if r['buyer_id'] else None), r['buyer_name']

    return None, None

async def revoke_buyer_access(ch, buyer_id=None):
    """關單時明確拔掉客人的頻道存取權限。
    使用會員自己的 Permission Overwrite，會覆蓋其原本透過 @everyone／角色取得的查看權。
    """
    try:
        if not buyer_id:
            buyer_id = (await buyer_for(ch))[0]
        if not buyer_id or not str(buyer_id).isdigit():
            return False, '找不到客人 ID'
        member = ch.guild.get_member(int(buyer_id))
        if member is None:
            try:
                member = await ch.guild.fetch_member(int(buyer_id))
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                member = None
        if member is None:
            return False, '客人目前不在伺服器成員快取／名單中'
        await ch.set_permissions(
            member,
            view_channel=False,
            send_messages=False,
            read_message_history=False,
            attach_files=False,
            embed_links=False,
            reason='CT小舖 關閉工單：移除客人存取權限'
        )
        return True, f'已移除 {member.display_name} 的工單存取權限'
    except (discord.Forbidden, discord.HTTPException) as e:
        print(f'[CLOSE_PERMISSION] 移除客人權限失敗｜channel={getattr(ch,"id",None)}｜buyer={buyer_id}｜error={e!r}')
        hint='（機器人需要「管理身分組」或管理員權限，才能修改頻道的成員權限；且機器人身分組必須高於客人的最高身分組）' if isinstance(e,discord.Forbidden) else ''
        return False, f'Discord 權限／API 錯誤：{e}{hint}'
    except Exception as e:
        import traceback; traceback.print_exc()
        return False, f'程式錯誤：{e}'

def rename_name(status,no,product,qty,buyer=None,service_type='幣號'):
    # V26：統一名稱格式：狀態-服務類型-工單號-額度
    st = '代肝' if service_type == '代肝' else '幣號'
    amount = str(product or '').strip()
    m = re.search(r'(\d+(?:\.\d+)?)\s*M', amount, re.I)
    if not m:
        try:
            amount = f'{int(qty)}M'
        except Exception:
            amount = '50M'
    else:
        amount = f'{m.group(1)}M'
    if st == '幣號':
        try:
            qn = max(1, int(qty))
        except (TypeError, ValueError):
            qn = 1
        return f'{status}-{st}-{no}-{amount}{qn}隻'
    return f'{status}-{st}-{no}-{amount}'


async def sync_ticket_state_after_rename(ch, old_status, new_status, buyer_id, service_type):
    """V78：/改名工單 的「結單／重新開放」同步處理。"""
    old_closed = str(old_status or '') == '結單'
    new_closed = str(new_status or '') == '結單'
    if new_closed:
        moved = await move_closed_ticket_to_service_category(ch, str(service_type or '幣號'))
        # buyer_id 空白時 revoke_buyer_access 會自己用 buyer_for 去找客人（訂單／頻道紀錄／歷史訊息）。
        ok, perm_msg = await revoke_buyer_access(ch, buyer_id or None)
        return (f'\n🔒 {perm_msg}' if ok else f'\n⚠️ 客人權限**沒有**拔除：{perm_msg}，請手動檢查頻道權限。') + \
               ('\n📁 已移入結單分類' if moved else '\n⚠️ 結單分類移動失敗（尚未設定分類、分類已滿，或機器人缺少管理頻道權限）')
    if old_closed and not new_closed:
        bid=str(buyer_id or '')
        if bid.isdigit():
            member=ch.guild.get_member(int(bid))
            if member:
                try:
                    await ch.set_permissions(
                        member,
                        view_channel=True,
                        send_messages=True,
                        read_message_history=True,
                        attach_files=True,
                        embed_links=True,
                        reason='CT小舖 改名工單離開結單狀態，恢復客人權限'
                    )
                except (discord.Forbidden, discord.HTTPException) as e:
                    print(f'[RENAME] restore buyer permission failed｜channel={ch.id}｜error={e!r}')
        try:
            open_cat=await get_ticket_creation_category(ch.guild)
            if isinstance(open_cat,discord.CategoryChannel) and ch.category_id != open_cat.id and len(open_cat.channels)<50:
                await ch.edit(category=open_cat,reason='CT小舖 改名工單離開結單分類')
        except (discord.Forbidden, discord.HTTPException) as e:
            print(f'[RENAME] move open category failed｜channel={ch.id}｜error={e!r}')

async def rename_order_channel(order, guild, reason='訂單狀態自動改名'):
    # 訂單成立／狀態變更時自動同步工單名稱，永遠不加入客人名稱。
    try:
        ch=guild.get_channel(int(order['channel_id']))
    except (TypeError, ValueError):
        return False
    if not isinstance(ch,discord.TextChannel):
        return False
    name=rename_name(order['status'],order['ticket_no'],order['product'],order['quantity'],service_type=order['service_type'] if 'service_type' in order.keys() else '幣號')
    if ch.name==name:
        return True
    return await _safe_edit_channel(ch,timeout=10,name=name,reason=reason)

def payment_info(g):
    vals=[('銀行',get(g,'pay_bank')),('代碼',get(g,'pay_code')),('帳號',get(g,'pay_account')),('戶名',get(g,'pay_name'))]
    s='\n'.join(f'{a}：{b}' for a,b in vals if b)
    return s or '目前尚未設定付款資訊，請聯絡店長。'

def template(g,k,default): return get(g,k,default)

async def post_channel_log(guild, setting_key, text):
    cid=get(guild.id,setting_key)
    if not cid: return
    ch=guild.get_channel(int(cid))
    if not isinstance(ch,discord.TextChannel): return
    try: await ch.send(text)
    except discord.HTTPException: pass

async def _status_announce_unlocked(order, guild):
    """建立或更新工單狀態頻道中的訂單卡。"""
    try:
        cid=get(guild.id,'status_channel_id','').strip()
        if not cid:
            print(f'[STATUS] 尚未設定狀態頻道｜guild={guild.id}｜order={order["id"]}')
            return False
        try:
            ch=guild.get_channel(int(cid))
        except (TypeError, ValueError):
            ch=None
        if ch is None:
            try:
                ch=await guild.fetch_channel(int(cid))
            except Exception as e:
                print(f'[STATUS] 找不到狀態頻道｜guild={guild.id}｜channel={cid}｜error={e!r}')
                return False
        if not isinstance(ch,discord.TextChannel):
            print(f'[STATUS] 設定的頻道不是文字頻道｜guild={guild.id}｜channel={cid}')
            return False

        buyer_name='客人'
        if order['buyer_id']:
            try:
                buyer=guild.get_member(int(order['buyer_id']))
                if buyer:
                    buyer_name=buyer.display_name
            except (TypeError, ValueError):
                pass
        if buyer_name=='客人':
            rec=q('SELECT buyer_name FROM ticket_channels WHERE channel_id=?',(str(order['channel_id']),),True)
            if rec and rec[0]['buyer_name']:
                buyer_name=rec[0]['buyer_name']

        delivery=order['delivery_time'] or get(guild.id,'delivery_time','') or '現貨'
        payment=order['payment_method'] or '尚未選擇'
        custom=template(guild.id,'status_template','')
        desc=''
        if custom:
            try:
                desc=custom.format(ticket=order['ticket_no'],status=order['status'],product=order['product'],quantity=order['quantity'],total=f"{order['total_price']:,}",customer=buyer_name,delivery=delivery,payment_method=payment)
            except Exception as e:
                print(f'[STATUS] 自訂狀態格式錯誤｜order={order["id"]}｜error={e!r}')
        if not desc:
            desc='請依照訂單狀態處理此筆訂單。'

        emb=discord.Embed(title=f'🧾 訂單 #{order["ticket_no"]}',description=desc)
        if order['service_type']=='代肝':
            emb.add_field(name='🛠️ 服務',value='代肝',inline=True)
            emb.add_field(name='📦 額度',value=order['product'],inline=True)
        else:
            emb.add_field(name='📦 商品',value=order['product'],inline=True)
            if not order['product'].startswith('不死號'): emb.add_field(name='🔢 數量',value=f'{order["quantity"]} 隻',inline=True)
        emb.add_field(name='💰 總價',value=f'NT${order["total_price"]:,}',inline=True)
        emb.add_field(name='👤 客人',value=buyer_name or '客人',inline=True)
        emb.add_field(name='📌 狀態',value=order['status'],inline=True)
        emb.add_field(name='💳 付款方式',value=payment,inline=True)
        emb.add_field(name='🕐 交貨時間',value=delivery,inline=False)
        emb.set_footer(text=f'工單 #{order["ticket_no"]}｜訂單 ID {order["id"]}')

        old=q('SELECT message_id,channel_id FROM status_posts WHERE order_id=?',(order['id'],),True)
        try:
            if old:
                try:
                    msg=await ch.fetch_message(int(old[0]['message_id']))
                    await msg.edit(content=None,embed=emb,view=OrderManageView(int(order['id'])))
                except discord.NotFound:
                    msg=await ch.send(embed=emb,view=OrderManageView(int(order['id'])))
                    q('UPDATE status_posts SET channel_id=?,message_id=?,updated_at=? WHERE order_id=?',(str(ch.id),str(msg.id),now(),order['id']))
                else:
                    q('UPDATE status_posts SET channel_id=?,updated_at=? WHERE order_id=?',(str(ch.id),now(),order['id']))
            else:
                msg=await ch.send(embed=emb,view=OrderManageView(int(order['id'])))
                q('INSERT OR REPLACE INTO status_posts(order_id,channel_id,message_id,updated_at) VALUES(?,?,?,?)',(order['id'],str(ch.id),str(msg.id),now()))
            return True
        except discord.Forbidden as e:
            print(f'[STATUS] 沒有權限在狀態頻道發送/編輯訊息｜guild={guild.id}｜channel={ch.id}｜order={order["id"]}｜error={e!r}')
            return False
        except discord.HTTPException as e:
            print(f'[STATUS] Discord API 錯誤｜guild={guild.id}｜channel={ch.id}｜order={order["id"]}｜error={e!r}')
            return False
        except Exception as e:
            print(f'[STATUS] 未預期錯誤｜guild={guild.id}｜channel={ch.id}｜order={order["id"]}｜error={e!r}')
            import traceback; traceback.print_exc()
            return False
    except Exception as e:
        print(f'[STATUS] 狀態公告流程錯誤｜order={order.get("id") if hasattr(order,"get") else "?"}｜error={e!r}')
        import traceback; traceback.print_exc()
        return False

async def status_announce(order, guild):
    # 同一訂單的狀態更新可能由付款、管理員按鈕、結單等流程同時觸發；
    # 用訂單級鎖避免兩條流程都判斷「沒有舊訊息」而重複發送狀態卡。
    oid=str(order.get('id','')) if hasattr(order,'get') else ''
    lk=STATUS_POST_LOCKS.setdefault(oid, asyncio.Lock())
    try:
        async with lk:
            return await _status_announce_unlocked(order, guild)
    finally:
        if STATUS_POST_LOCKS.get(oid) is lk:
            STATUS_POST_LOCKS.pop(oid,None)

def log_order(order_id,gid,op,action,detail=''): q('INSERT INTO order_logs(order_id,guild_id,operator_id,action,detail,created_at) VALUES(?,?,?,?,?,?)',(str(order_id),str(gid),str(op) if op else None,action,detail,now()))

async def remove_obsolete_bot_panels(guild):
    """刪除目前版本以前留下的互動面板。只處理本 Bot 自己發出的訊息。

    現行持久化互動 custom_id 統一以 ct_ 開頭；ephemeral/短生命週期按鈕不會出現在頻道歷史。
    因此舊版使用 vXX_*、rename_* 等 custom_id 的面板可安全清除，不碰其他 Bot。
    """
    removed=0
    bot_user_id=getattr(getattr(bot,'user',None),'id',None)
    if not bot_user_id:
        return 0
    for ch in list(getattr(guild,'text_channels',[]) or []):
        try:
            async for msg in ch.history(limit=2000, oldest_first=True):
                if getattr(getattr(msg,'author',None),'id',None) != bot_user_id:
                    continue
                components=getattr(msg,'components',[]) or []
                if not components:
                    continue
                ids=[]
                for row in components:
                    for child in (getattr(row,'children',[]) or []):
                        cid=getattr(child,'custom_id',None)
                        if cid: ids.append(str(cid))
                # 只有所有可識別的持久化元件都不是目前 ct_ 系列時才刪除。
                # 無 custom_id 的舊元件也視為舊面板；目前頻道內的持久化面板皆有 ct_ ID。
                if not ids or any(not x.startswith('ct_') for x in ids):
                    try:
                        await msg.delete(reason='CT小舖 清理舊版互動面板')
                        removed += 1
                    except (discord.HTTPException, discord.NotFound, discord.Forbidden):
                        pass
        except (discord.HTTPException, discord.Forbidden):
            continue
    return removed

async def send_purchase_panel(ch):
    """新工單唯一自動顯示的購買服務面板；不會顯示改名工單面板。"""
    if not isinstance(ch, discord.TextChannel): return False
    no=ticket_no(ch.name)
    if no is None:
        rec=ticket_record(ch)
        no=rec['ticket_no'] if rec else None
    if no is None: return False
    # 已有購買服務面板就不重複發送。
    try:
        async for msg in ch.history(limit=100, oldest_first=True):
            if not getattr(getattr(msg, 'author', None), 'bot', False):
                continue
            ids=[getattr(child,'custom_id',None) for row in (getattr(msg,'components',[]) or []) for child in (getattr(row,'children',[]) or [])]
            if 'ct_service:coin' in ids or 'ct_service:boost' in ids:
                return True
    except discord.HTTPException:
        pass
    desc='請先選擇您要購買的服務。\n\n🪙 **幣號**\n購買遊戲幣號\n\n🛠️ **代肝**\n選擇代肝額度並付款排單'
    if off_hours(ch.guild.id):
        desc += '\n\n🕐 **目前為非營業時間**\n目前可以正常下單及付款，但付款後會等店長回來再處理。'
    emb=discord.Embed(title='🛒 三角洲交易系統',description=desc)
    emb.set_footer(text=f'工單 #{no}')
    try:
        await ch.send(content='🛒 三角洲交易系統',embed=emb,view=ServiceTypeView())
        return True
    except discord.HTTPException as e:
        print('[PURCHASE_PANEL] 發送失敗:',repr(e))
        return False

class ServiceTypeView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        b=discord.ui.Button(label='🪙 幣號',style=discord.ButtonStyle.primary,custom_id='ct_service:coin')
        b.callback=show_coin_menu; self.add_item(b)
        b=discord.ui.Button(label='🛠️ 代肝',style=discord.ButtonStyle.primary,custom_id='ct_service:boost')
        b.callback=show_boost_menu; self.add_item(b)

async def show_coin_menu(i):
    if not isinstance(i.channel,discord.TextChannel):
        return await i.response.send_message('❌ 請在工單頻道使用。',ephemeral=True)
    desc=f'📋 幣號價目表：{channel_link(i.guild,"幣號價目表")}\n\n請選擇您要的幣號規格。\n🟢 正常提供｜🟡 暫停提供｜🔴 缺貨'
    e=discord.Embed(title='🪙 幣號',description=desc)
    await i.response.edit_message(content=None,embed=e,view=CoinProductView())

async def show_boost_menu(i):
    if not isinstance(i.channel,discord.TextChannel):
        return await i.response.send_message('❌ 請在工單頻道使用。',ephemeral=True)
    lines=[f'📋 代肝價目表：{channel_link(i.guild,"代肝價目表")}','\n請選擇代肝額度：']
    for t in BOOST_TIERS:
        v=boost_price(i.guild.id,t); lines.append(f'{t}：NT${v:,}' if v>0 else f'{t}：尚未設定')
    e=discord.Embed(title='🛠️ 代肝',description='\n'.join(lines))
    await i.response.edit_message(content=None,embed=e,view=BoostTierView())

class OpenTicketView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        b=discord.ui.Button(label='🎫 開始開單',style=discord.ButtonStyle.primary,custom_id='ct_open_ticket'); b.callback=self.open_ticket; self.add_item(b)
    async def open_ticket(self,i):
        # V110：按鈕回呼第一時間就 ACK，避免排隊鎖／Discord API 延遲導致「交互失敗」。
        if not i.guild or not isinstance(i.user, discord.Member):
            return await i.response.send_message('❌ 無法在此使用。',ephemeral=True)
        try:
            await i.response.defer(ephemeral=True)
        except (discord.NotFound, discord.HTTPException) as e:
            print('[TICKET] open initial ACK failed:',repr(e))
            return
        key=(i.guild.id,i.user.id)
        lk=OPEN_TICKET_LOCKS.setdefault(key, asyncio.Lock())
        if lk.locked():
            return await i.followup.send('⏳ 正在建立您的工單，請稍候，不要重複按開單。',ephemeral=True)
        async with lk:
            try:
                return await self._open_ticket_impl(i)
            finally:
                OPEN_TICKET_LOCKS.pop(key,None)

    async def _open_ticket_impl(self,i):
        # V110：open_ticket 已經完成 Interaction ACK；後續一律使用 followup。
        acknowledged=True
        try:
            if not i.guild or not isinstance(i.user,discord.Member):
                return await i.followup.send('❌ 無法在此使用。',ephemeral=True)

            # 先確認機器人本身有建立頻道與設定權限的能力。
            me=i.guild.me
            if me is None:
                return await i.followup.send('❌ 找不到機器人的伺服器身分，請稍後再試。',ephemeral=True)
            perms=(i.channel.permissions_for(me) if isinstance(i.channel,discord.TextChannel) else i.guild.me.guild_permissions)
            if not perms.manage_channels:
                return await i.followup.send('❌ 機器人缺少「管理頻道」權限，無法建立工單。',ephemeral=True)

            # 預設同一位客人只能保留一張未結單工單；店長開啟「無限制開單」後，
            # 這一層限制完全跳過，但仍保留 OPEN_TICKET_LOCKS 防止同一時間快速連點造成重複建立。
            unlimited_open = get(i.guild.id,'unlimited_ticket_opening','0') == '1'
            if not unlimited_open:
                checked=set()
                for r in q('SELECT channel_id FROM ticket_channels WHERE guild_id=? AND buyer_id=?',(str(i.guild.id),str(i.user.id)),True):
                    try: ch0=i.guild.get_channel(int(r['channel_id']))
                    except (TypeError,ValueError): ch0=None
                    if not isinstance(ch0,discord.TextChannel):
                        continue
                    checked.add(ch0.id)
                    # 歷史 ticket_channels 不等於目前開啟中的工單；以最新訂單狀態 + Discord 頻道狀態共同判斷。
                    active_order=q("SELECT status FROM orders WHERE channel_id=? AND buyer_id=? ORDER BY id DESC LIMIT 1",(str(ch0.id),str(i.user.id)),True)
                    if active_order and active_order[0]['status'] in ('結單','已取消'):
                        continue
                    if looks_closed(ch0.name):
                        continue
                    return await i.followup.send(f'❌ 你目前已有開啟中的工單：{ch0.mention}',ephemeral=True)
                for ch0 in i.guild.text_channels:
                    if ch0.id in checked or not is_ticket_candidate(ch0) or looks_closed(ch0.name):
                        continue
                    try:
                        if ch0.permissions_for(i.user).view_channel:
                            # 只有能被該會員實際看見的「工單候選」才視為既有工單。
                            remember(ch0,ticket_no(ch0.name) or '0',i.user.id,i.user.display_name)
                            return await i.followup.send(f'❌ 你目前已有開啟中的工單：{ch0.mention}',ephemeral=True)
                    except Exception:
                        continue

            category=await get_ticket_creation_category(i.guild)
            if not isinstance(category,discord.CategoryChannel) and get(i.guild.id,'service_category_enabled','0')!='1' and isinstance(i.channel,discord.TextChannel):
                category=i.channel.category
            if get(i.guild.id,'service_category_enabled','0')=='1' and not isinstance(category,discord.CategoryChannel):
                return await i.followup.send('❌ 今天的工單分類建立失敗，請確認機器人有「管理頻道」權限後再試。',ephemeral=True)

            no=await allocate_ticket_no(i.guild.id)
            ow={
                i.guild.default_role:discord.PermissionOverwrite(view_channel=False),
                i.user:discord.PermissionOverwrite(view_channel=True,send_messages=True,read_message_history=True,attach_files=True)
            }
            owner=get(i.guild.id,'ticket_owner_id',''); rid=get(i.guild.id,'ticket_staff_role_id','')
            if owner.isdigit() and (m:=i.guild.get_member(int(owner))):
                ow[m]=discord.PermissionOverwrite(view_channel=True,send_messages=True,read_message_history=True,manage_messages=True,attach_files=True,embed_links=True)
            if rid.isdigit() and (role:=i.guild.get_role(int(rid))):
                ow[role]=discord.PermissionOverwrite(view_channel=True,send_messages=True,read_message_history=True,attach_files=True,embed_links=True)
            ow[me]=discord.PermissionOverwrite(view_channel=True,send_messages=True,read_message_history=True,manage_channels=True,manage_messages=True,attach_files=True,embed_links=True)

            ch=await i.guild.create_text_channel(f'ticket-{no}',category=category,overwrites=ow,reason='CT小舖開單')
            remember(ch,no,i.user.id,i.user.display_name)
            # 新工單要保留「購買服務」面板：幣號／代肝 → 額度／數量 → 付款。
            panel_ok=await send_purchase_panel(ch)
            try:
                await ch.edit(name=f'工單-{no}',reason='新工單自動改名')
            except discord.HTTPException as e:
                print('[TICKET] rename error:',repr(e))
            # V68：新工單永遠留在「設定開單分類」，不因選擇幣號／代肝而移動。

            msg=f'✅ 工單已建立：{ch.mention}\n購買服務面板已送出。\n店長如需修改工單名稱，請使用 `/改名工單`。'
            if not panel_ok:
                msg+='\n⚠️ 購買服務面板發送失敗，請檢查 Bot 的發送訊息權限。'
            await i.followup.send(msg,ephemeral=True)
        except discord.Forbidden as e:
            print('[TICKET] open forbidden:',repr(e))
            if acknowledged:
                await i.followup.send('❌ 機器人權限不足，無法完成開單。請確認機器人有「管理頻道」及「查看頻道／發送訊息」權限。',ephemeral=True)
            else:
                await i.response.send_message('❌ 機器人權限不足，無法完成開單。',ephemeral=True)
        except discord.HTTPException as e:
            print('[TICKET] open HTTP error:',repr(e))
            if acknowledged:
                await i.followup.send('❌ Discord 暫時拒絕了開單操作，請稍後再按一次。',ephemeral=True)
            else:
                await i.response.send_message('❌ Discord 暫時拒絕了開單操作，請稍後再試。',ephemeral=True)
        except Exception as e:
            import traceback
            print('[TICKET] open unexpected error:',repr(e))
            traceback.print_exc()
            if acknowledged:
                await i.followup.send('❌ 開單時發生程式錯誤，已記錄錯誤資訊，請通知店長。',ephemeral=True)
            else:
                await i.response.send_message('❌ 開單時發生程式錯誤，請通知店長。',ephemeral=True)

class WelcomeCloseView(discord.ui.View):
    """新工單第一則訊息：只有關閉工單，避免一開單出現多個面板。"""
    def __init__(self):
        super().__init__(timeout=None)
        b = discord.ui.Button(label='🔒 關閉工單', style=discord.ButtonStyle.danger, custom_id='ct_welcome_close')
        b.callback = self.close
        self.add_item(b)

    async def close(self, i):
        rec = ticket_record(i.channel) if isinstance(i.channel, discord.TextChannel) else None
        buyer_id = str(rec['buyer_id']) if rec and rec['buyer_id'] else ''
        if not admin(i.user) and str(i.user.id) != buyer_id:
            return await i.response.send_message('❌ 只有這張工單的客人或管理員可以關閉工單。', ephemeral=True)
        text=('⚠️ **確定要關閉此工單嗎？**\n關閉後您將無法查看此工單，管理員仍可重新開放。' if str(i.user.id)==buyer_id and not admin(i.user) else '⚠️ **確定要關閉此工單嗎？**\n關閉後該客人將無法查看此工單，管理員仍可重新開放。')
        await i.response.send_message(text,view=CloseConfirmView(buyer_id=buyer_id),ephemeral=True)

class TicketControlView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        b=discord.ui.Button(label='📋 工單狀況',style=discord.ButtonStyle.primary,custom_id='ct_ticket_status'); b.callback=self.status; self.add_item(b)
        b=discord.ui.Button(label='🔒 關閉工單',style=discord.ButtonStyle.danger,custom_id='ct_ticket_close'); b.callback=self.close; self.add_item(b)
    async def status(self,i):
        if not admin(i.user): return await i.response.send_message('❌ 只有管理員可以操作。',ephemeral=True)
        r=q('SELECT * FROM orders WHERE channel_id=? ORDER BY id DESC LIMIT 1',(str(i.channel.id),),True)
        if not r: return await i.response.send_message('📋 目前尚未建立訂單。',ephemeral=True)
        await i.response.send_message(embed=order_status_embed(r[0]),view=OrderStatusButtons(r[0]['id']),ephemeral=True)
    async def close(self,i):
        # 關閉工單允許「管理員」或「這張工單的客人」操作。
        # 狀態變更仍只開放管理員，避免客人修改訂單狀態。
        rec=ticket_record(i.channel) if isinstance(i.channel,discord.TextChannel) else None
        buyer_id=str(rec['buyer_id']) if rec and rec['buyer_id'] else ''
        if not admin(i.user) and str(i.user.id) != buyer_id:
            return await i.response.send_message('❌ 只有這張工單的客人或管理員可以關閉工單。',ephemeral=True)
        text=('⚠️ **確定要關閉此工單嗎？**\n關閉後您將無法查看此工單，管理員仍可重新開放。' if str(i.user.id)==buyer_id and not admin(i.user) else '⚠️ **確定要關閉此工單嗎？**\n關閉後該客人將無法查看此工單，管理員仍可重新開放。')
        await i.response.send_message(text,view=CloseConfirmView(buyer_id=buyer_id),ephemeral=True)

class CloseConfirmView(discord.ui.View):
    def __init__(self,buyer_id=''):
        super().__init__(timeout=60)
        self.buyer_id=str(buyer_id or '')
        for label,style,cb in [('❌ 取消',discord.ButtonStyle.secondary,self.cancel),('🔒 確認關閉',discord.ButtonStyle.danger,self.confirm)]:
            b=discord.ui.Button(label=label,style=style); b.callback=cb; self.add_item(b)
    async def cancel(self,i): await i.response.edit_message(content='已取消關閉工單。',view=None)
    async def confirm(self,i):
        ch=i.channel
        if not admin(i.user) and str(i.user.id) != self.buyer_id:
            return await i.response.edit_message(content='❌ 你沒有權限關閉這張工單。',view=None)
        if not isinstance(ch,discord.TextChannel): return await i.response.edit_message(content='❌ 找不到工單。',view=None)
        await i.response.defer(ephemeral=True)
        rec=ticket_record(ch); buyer_id=rec['buyer_id'] if rec else ''

        # 關單時同步把訂單狀態設為「結單」，並依結單狀態重新命名工單。
        # 如果原本已經是「結單」，仍會套用同一個名稱；不會產生重複或額外後綴。
        rows=q('SELECT * FROM orders WHERE channel_id=? ORDER BY id DESC LIMIT 1',(str(ch.id),),True)
        if rows:
            o=rows[0]
            old_status=o['status']
            if old_status != '結單':
                q('UPDATE orders SET status=?,completed_at=? WHERE id=?',('結單',now(),o['id']))
            elif not o['completed_at']:
                q('UPDATE orders SET completed_at=? WHERE id=?',(now(),o['id']))
            closed_order={**dict(o), 'status':'結單'}
            await rename_order_channel(closed_order, ch.guild, reason='工單關閉自動改名')
            service_type=str(o['service_type'] or '幣號') if 'service_type' in o.keys() else '幣號'
            moved=await move_closed_ticket_to_service_category(ch, service_type)
            if not moved:
                await asyncio.sleep(0.2)
                moved=await move_closed_ticket_to_service_category(ch, service_type)
            log_order(o['id'],ch.guild.id,i.user.id,'關閉工單',f'{old_status} -> 結單｜移至{service_type}結單分類')
            await post_channel_log(ch.guild,'action_log_channel_id',f'🔒 **關閉工單**｜#{o["ticket_no"]}｜{old_status} → 結單｜{i.user.mention}')
        else:
            # 舊／外部工單沒有訂單資料時，也要完成「結單、分類、拔權限」。
            service_type=_service_from_name(ch.name) or '幣號'
            ticket_number=_resolve_ticket_number(ch) or '0000'
            product,qty=_amount_qty_from_name(ch.name,service_type)
            product=product or '50M'; qty=max(1,int(qty or 1))
            final_name=rename_name('結單',ticket_number,product,qty,service_type=service_type)
            if ch.name!=final_name: await _safe_edit_channel(ch,timeout=10,name=final_name,reason='工單關閉自動改名')
            moved=await move_closed_ticket_to_service_category(ch,service_type)
            if not moved:
                await asyncio.sleep(0.2)
                moved=await move_closed_ticket_to_service_category(ch,service_type)

        ok,perm_msg=await revoke_buyer_access(ch,buyer_id)
        if not ok:
            print(f'[CLOSE_CONFIRM] {perm_msg}')

        # 這裡是按鈕 Interaction，不要用 followup.edit_message() 去編輯 ephemeral 的原始回覆。
        # ephemeral 回覆應使用 edit_original_response()；否則 Discord 可能回 Unknown Message，
        # 最後就會在畫面上只看到「指令執行失敗」。
        try:
            await ch.send(
                f'🔒 **工單已關閉**\n此工單已設為「結單」。\n🔒 {perm_msg}\n管理員仍可重新開放或刪除工單。',
                view=ClosedTicketView()
            )
        except (discord.Forbidden, discord.HTTPException) as e:
            print(f'[CLOSE_CONFIRM] 關閉後訊息發送失敗｜channel={ch.id}｜error={e!r}')

        try:
            await i.edit_original_response(
                content=f'✅ 工單已關閉，狀態已設為「結單」。\n{perm_msg}',
                view=None
            )
        except (discord.NotFound, discord.HTTPException) as e:
            print(f'[CLOSE_CONFIRM] 更新確認訊息失敗｜message={getattr(i.message, "id", None)}｜error={e!r}')

class ClosedTicketView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        b=discord.ui.Button(label='🔓 重新開放',style=discord.ButtonStyle.success,custom_id='ct_ticket_reopen'); b.callback=self.reopen; self.add_item(b)
        b=discord.ui.Button(label='🗑️ 刪除工單',style=discord.ButtonStyle.danger,custom_id='ct_ticket_delete'); b.callback=self.delete; self.add_item(b)
    async def reopen(self,i):
        if not admin(i.user): return await i.response.send_message('❌ 只有管理員可以操作。',ephemeral=True)
        await i.response.defer(ephemeral=True)
        buyer_id=(await buyer_for(i.channel))[0] if isinstance(i.channel,discord.TextChannel) else None
        if buyer_id and str(buyer_id).isdigit():
            m=i.guild.get_member(int(buyer_id))
            if m is None:
                try: m=await i.guild.fetch_member(int(buyer_id))
                except (discord.NotFound,discord.Forbidden,discord.HTTPException): m=None
            if m:
                try:
                    await i.channel.set_permissions(m,view_channel=True,send_messages=True,read_message_history=True,attach_files=True,embed_links=True,reason='工單重新開放')
                except (discord.Forbidden,discord.HTTPException) as e:
                    print(f'[TICKET_REOPEN] 恢復客人權限失敗｜channel={i.channel.id}｜error={e!r}')
        rows=q('SELECT * FROM orders WHERE channel_id=? ORDER BY id DESC LIMIT 1',(str(i.channel.id),),True)
        reopened_status=None
        if rows:
            o=rows[0]
            reopened_status='待處理'
            q('UPDATE orders SET status=?,completed_at=NULL WHERE id=?',(reopened_status,o['id']))
            rr=q('SELECT * FROM orders WHERE id=?',(o['id'],),True)[0]
            try:
                await asyncio.wait_for(i.channel.edit(name=rename_name(reopened_status,rr['ticket_no'],rr['product'],rr['quantity'],service_type=rr['service_type'] or '幣號'),reason='工單重新開放自動改名'),timeout=20)
            except (discord.Forbidden,discord.HTTPException,asyncio.TimeoutError) as e:
                q('UPDATE orders SET status=?,completed_at=? WHERE id=?',(o['status'],o['completed_at'],o['id']))
                print('[TICKET_REOPEN] 改名失敗，DB 已回復:',repr(e))
                return await i.followup.send(f'❌ 重新開放失敗，工單資料已回復：`{str(e)[:180]}`',ephemeral=True)
            await status_announce(rr,i.guild)
        try:
            open_cat=await get_ticket_creation_category(i.guild)
            if isinstance(open_cat,discord.CategoryChannel) and i.channel.category_id != open_cat.id:
                if len(open_cat.channels) < 50:
                    await i.channel.edit(category=open_cat,reason='工單重新開放移回開單分類')
        except (discord.Forbidden,discord.HTTPException) as e:
            print('[TICKET_REOPEN] 移回開單分類失敗:',repr(e))
        await i.followup.send(f'🔓 工單已重新開放，狀態：**{reopened_status or "待處理"}**。並移回開單分類。',ephemeral=True)
    async def delete(self,i):
        if not admin(i.user): return await i.response.send_message('❌ 只有管理員可以操作。',ephemeral=True)
        await i.response.send_message('⚠️ **確定要永久刪除此工單嗎？**',view=DeleteConfirmView(),ephemeral=True)

class DeleteConfirmView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=60)
        for label,style,cb in [('❌ 取消',discord.ButtonStyle.secondary,self.cancel),('🗑️ 確定刪除',discord.ButtonStyle.danger,self.confirm)]:
            b=discord.ui.Button(label=label,style=style); b.callback=cb; self.add_item(b)
    async def cancel(self,i): await i.response.edit_message(content='已取消刪除工單。',view=None)
    async def confirm(self,i):
        if not admin(i.user): return await i.response.edit_message(content='❌ 權限不足。',view=None)
        await i.response.edit_message(content='🗑️ 正在刪除工單……',view=None)
        channel_id=str(i.channel.id)
        try:
            await i.channel.delete(reason=f'工單刪除｜{i.user}')
            TICKET_CHANNEL_IDS.discard(channel_id)
        except discord.HTTPException: pass

def order_status_embed(o):
    e=discord.Embed(title=f'📋 工單狀況｜#{o["ticket_no"]}',description='管理員可直接按下方按鈕更新狀態。')
    e.add_field(name='服務',value=o['service_type'],inline=True); e.add_field(name='商品／額度',value=o['product'],inline=True); e.add_field(name='數量',value=f'{o["quantity"]} 隻',inline=True)
    e.add_field(name='金額',value=f'NT${int(o["total_price"]):,}',inline=True); e.add_field(name='狀態',value=o['status'],inline=True); e.add_field(name='付款明細',value='已提供' if o['payment_proof_url'] else '尚未提供',inline=True)
    return e

class OrderStatusButtons(discord.ui.View):
    def __init__(self,oid):
        super().__init__(timeout=120); self.oid=oid
        for st in ['待確認付款','待排單','待交貨','處理中','待收貨','有爭議','已完成']:
            b=discord.ui.Button(label=st,style=discord.ButtonStyle.danger if st=='有爭議' else discord.ButtonStyle.primary); b.callback=self.make(st); self.add_item(b)
    def make(self,st):
        async def cb(i):
            if not admin(i.user): return await i.response.send_message('❌ 只有管理員可以操作。',ephemeral=True)
            await i.response.defer(ephemeral=True)
            r=q('SELECT * FROM orders WHERE id=?',(self.oid,),True)
            if not r: return await i.followup.send('❌ 找不到訂單。',ephemeral=True)
            o=r[0]; new='結單' if st=='已完成' else st
            q('UPDATE orders SET status=?,completed_at=? WHERE id=?',(new,now() if new=='結單' else None,self.oid)); log_order(self.oid,i.guild.id,i.user.id,'按鈕修改狀態',f'{o["status"]} → {new}')
            rr=q('SELECT * FROM orders WHERE id=?',(self.oid,),True)[0]; await rename_order_channel(rr,i.guild,'工單狀態按鈕自動改名'); await status_announce(rr,i.guild)
            await i.followup.send(f'📋 **工單狀態更新**\n#{o["ticket_no"]}\n{o["status"]} → **{new}**', ephemeral=True)
        return cb

class CoinProductView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        for p in COIN_PRODUCTS:
            self.add_item(CoinProductButton(p))

class CoinProductButton(discord.ui.Button):
    def __init__(self,p):
        self.p=p
        st=availability(p) if p in [r['product'] for r in q('SELECT product FROM products',(),True)] else '正常提供'
        icon={'正常提供':'🟢','暫停提供':'🟡','缺貨':'🔴'}.get(st,'🟢')
        super().__init__(label=f'{icon} {p}',style=discord.ButtonStyle.primary,custom_id=f'ct_coin:{p}')
    async def callback(self,i):
        st=availability(self.p)
        if st=='暫停提供': return await i.response.send_message('⚠️ **目前暫停提供**\n此規格目前暫時不提供，請選擇其他規格。',ephemeral=True)
        if st=='缺貨': return await i.response.send_message('🔴 **目前缺貨**\n此規格目前沒有現貨，請選擇其他規格。',ephemeral=True)
        if '不死號' in self.p:
            await create_negotiation_order(i,self.p)
            return
        await i.response.send_modal(QuantityModal(self.p,i.message.id))

class BoostTierView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        for t in BOOST_TIERS:
            b=discord.ui.Button(label=t,style=discord.ButtonStyle.primary,custom_id=f'ct_boost:{t}')
            b.callback=self.make_cb(t); self.add_item(b)
    def make_cb(self,tier):
        async def cb(i):
            await create_boost_order(i,tier)
        return cb

class QuantityModal(discord.ui.Modal,title='輸入購買數量'):
    # 與客人「自動購買 → 幣號 → 選擇要買幾隻」共用同一個原生 Discord 數量輸入視窗。
    qty=discord.ui.TextInput(label='購買數量',placeholder='請輸入數量，例如：7',min_length=1,max_length=6,required=True)
    def __init__(self,p=None,source_message_id=None):
        super().__init__()
        self.p=p
        self.source_message_id=int(source_message_id) if source_message_id is not None else None

    async def on_submit(self,i):
        try:
            n=int(str(self.qty.value).strip())
        except (TypeError,ValueError):
            return await i.response.send_message('❌ 數量請輸入整數，例如：7。',ephemeral=True)
        if not 1<=n<=999999:
            return await i.response.send_message('❌ 數量請輸入 1～999999 的整數。',ephemeral=True)

        # 客人自動購買的原本流程完全不變。
        await i.response.defer()
        await process_selection(i,self.p,n,source_message_id=self.source_message_id)

def quantity_embed(p,n,gid):
    unit=price(p); total=unit*n if unit>0 else 0
    desc=f'商品：**{p}**\n數量：**{n} 隻**'
    if unit>0: desc += f'\n單價：NT${unit:,}\n總價：**NT${total:,}**'
    else: desc += '\n⚠️ 此商品目前尚未設定價格。'
    return discord.Embed(title='📦 訂單資訊',description=desc)

async def edit_source_message(i,source_message_id,*,content=None,embed=None,view=None):
    if source_message_id is None: return await i.edit_original_response(content=content,embed=embed,view=view)
    ch=i.channel
    if not isinstance(ch,discord.TextChannel): return False
    try:
        msg=ch.get_partial_message(int(source_message_id)); await msg.edit(content=content,embed=embed,view=view); return True
    except (discord.NotFound,discord.Forbidden,discord.HTTPException) as e: print('edit_source_message error:',repr(e)); return False

async def process_selection(i,p,n,source_message_id=None):
    ch=i.channel
    if not isinstance(ch,discord.TextChannel): return await i.followup.send('❌ 請在工單頻道操作。',ephemeral=True) if i.response.is_done() else await i.response.send_message('❌ 請在工單頻道操作。',ephemeral=True)
    rec=ticket_record(ch); no=rec['ticket_no'] if rec else ticket_no(ch.name)
    if not no: return await i.followup.send('❌ 暫時無法辨識工單編號，請稍後再試。',ephemeral=True)
    if availability(p)!='正常提供': return await i.followup.send('❌ 此規格目前無法購買。',ephemeral=True)
    try: min_qty=int(get(ch.guild.id,f'min_purchase_qty:{p}','0') or 0)
    except (TypeError,ValueError): min_qty=0
    if min_qty>0 and n<min_qty:
        return await i.followup.send(f'❌ 此規格最低購買數量為 **{min_qty} 隻**，目前輸入 {n} 隻。',ephemeral=True)
    unit=price(p)
    if unit<=0: return await i.followup.send('❌ 這項商品目前尚未設定價格，請聯絡店長。',ephemeral=True)
    s=stock(p)
    if s<n:
        delivery=get(ch.guild.id,'delivery_time','').strip()
        if not delivery: return await i.followup.send(f'❌ 目前 {p} 庫存不足，店長尚未設定交貨時間。',ephemeral=True)
        e=discord.Embed(title='📦 缺貨訂單確認',description=f'目前僅剩 {s} 隻現貨，您需要 {n} 隻。\n\n🕐 本店交貨時間：**{delivery}**\n\n請確認是否接受。')
        e.add_field(name='商品',value=p); e.add_field(name='數量',value=f'{n} 隻'); e.add_field(name='總價',value=f'NT${unit*n:,}')
        ok=await edit_source_message(i,source_message_id,embed=e,view=DeliveryAcceptView(p,n,unit,unit*n,delivery))
        if not ok:
            await i.followup.send(embed=e,view=DeliveryAcceptView(p,n,unit,unit*n,delivery),ephemeral=True)
        return None
    return await create_order_and_show_payment(i,p,n,unit,unit*n,'',source_message_id=source_message_id,already_deferred=True)

async def create_order_and_show_payment(i,p,qty,unit,total,delivery,source_message_id=None,already_deferred=False,service_type='幣號'):
    if not already_deferred:
        try: await i.response.defer()
        except discord.InteractionResponded: pass
    ch=i.channel
    if not isinstance(ch,discord.TextChannel): return await i.edit_original_response(content='❌ 請在工單頻道操作。',embed=None,view=None)
    rec=ticket_record(ch); no=rec['ticket_no'] if rec else ticket_no(ch.name)
    if not no: return await i.edit_original_response(content='❌ 暫時無法辨識工單編號，請稍後再試。',embed=None,view=None)
    existing=q("SELECT * FROM orders WHERE channel_id=? AND buyer_id=? AND status NOT IN ('結單','已取消') ORDER BY id DESC LIMIT 1",(str(ch.id),str(i.user.id)),True)
    if existing: return await i.edit_original_response(content=f'ℹ️ 這張工單已有進行中的訂單 **#{existing[0]["ticket_no"]}**，目前狀態：**{existing[0]["status"]}**。',embed=None,view=None)
    if service_type=='幣號' and availability(p)!='正常提供': return await i.edit_original_response(content='❌ 此規格目前無法購買。',embed=None,view=None)
    stock_reserved=0
    with db:
        c=db.cursor()
        if service_type=='幣號' and not str(p).startswith('不死號'):
            # 只有現貨足夠時才預扣庫存；缺貨接受交貨時間的訂單不把庫存扣成負數。
            c.execute('UPDATE products SET stock=stock-? WHERE product=? AND stock>=?',(int(qty),p,int(qty)))
            if c.rowcount==1: stock_reserved=1
            elif int(stock(p))>=int(qty):
                raise sqlite3.IntegrityError('庫存在建立訂單瞬間發生競爭，請重新操作。')
        c.execute('INSERT INTO orders(ticket_no,channel_id,guild_id,product,quantity,unit_price,total_price,status,buyer_id,created_at,delivery_time,service_type,stock_reserved) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',(no,str(ch.id),str(ch.guild.id),p,qty,unit,total,'待付款',str(i.user.id),now(),delivery,service_type,stock_reserved)); oid=c.lastrowid; db.commit()
    order=q('SELECT * FROM orders WHERE id=?',(oid,),True)[0]
    await rename_order_channel(order,ch.guild,'建立訂單後自動改名')
    await post_channel_log(ch.guild,'order_log_channel_id',f'🧾 **新{service_type}訂單**｜#{no}｜{p}｜NT${total:,}｜客人：{i.user.mention}')
    log_order(oid,ch.guild.id,i.user.id,'建立訂單',f'{service_type}｜{p}｜NT${total:,}')
    await status_announce(order,ch.guild)
    e=discord.Embed(title='💳 付款方式',description=template(ch.guild.id,'pay_method_intro','本店付款方式為 {bank}\n\n請問您要使用的付款方式是？').format(bank=get(ch.guild.id,'pay_bank','尚未設定')))
    e.add_field(name='訂單',value=f'#{no}｜{p}｜NT${total:,}',inline=False)
    if off_hours(ch.guild.id): e.add_field(name='🕐 非營業時間',value='目前可以付款，但店長目前不在線，付款後會等店長回來確認。',inline=False)
    if source_message_id is not None:
        ok=await edit_source_message(i,source_message_id,embed=e,view=PaymentMethodView(oid))
        if not ok:
            await i.followup.send(embed=e,view=PaymentMethodView(oid),ephemeral=True)
        return
    await i.edit_original_response(embed=e,view=PaymentMethodView(oid))

async def create_boost_order(i,tier):
    price_v=boost_price(i.guild.id,tier)
    if price_v<=0: return await i.response.send_message('❌ 此代肝額度目前尚未設定價格，請聯絡店長。',ephemeral=True)
    await i.response.defer()
    return await create_order_and_show_payment(i,tier,1,price_v,price_v,'',already_deferred=True,service_type='代肝')

async def create_negotiation_order(i,p):
    await i.response.defer()
    ch=i.channel; no=ticket_no(ch.name)
    existing=q("SELECT * FROM orders WHERE channel_id=? AND buyer_id=? AND status NOT IN ('結單','已取消') ORDER BY id DESC LIMIT 1",(str(ch.id),str(i.user.id)),True)
    if existing: return await i.followup.send(f'ℹ️ 此工單已有進行中的訂單 #{existing[0]["ticket_no"]}。',ephemeral=True)
    c=db.cursor(); c.execute('INSERT INTO orders(ticket_no,channel_id,guild_id,product,quantity,unit_price,total_price,status,buyer_id,created_at,service_type) VALUES(?,?,?,?,?,?,?,?,?,?,?)',(no,str(ch.id),str(ch.guild.id),p,1,0,0,'待洽談',str(i.user.id),now(),'幣號')); oid=c.lastrowid; db.commit()
    order=q('SELECT * FROM orders WHERE id=?',(oid,),True)[0]
    await rename_order_channel(order,ch.guild,'不死號洽談')
    await ch.send('📋 **不死號**\n已通知店長，請稍候，店長會與您洽談。')
    await post_channel_log(ch.guild,'order_log_channel_id',f'📋 **不死號洽談通知**\n工單：{ch.mention}\n客人：{i.user.mention}\n客人選擇了不死號，請前往工單洽談。')
    await status_announce(order,ch.guild)
    await i.edit_original_response(content='📋 已通知店長，請稍候，店長會與您洽談。',embed=None,view=None)

class DeliveryAcceptView(discord.ui.View):
    def __init__(self,p,qty,unit,total,delivery):
        super().__init__(timeout=300); self.p=p; self.qty=qty; self.unit=unit; self.total=total; self.delivery=delivery
    @discord.ui.button(label='✅ 可以，接受時間',style=discord.ButtonStyle.success)
    async def accept(self,i,button):
        if price(self.p)<=0: return await i.response.send_message('❌ 商品價格已變更，請重新選購。',ephemeral=True)
        current_delivery=get(i.guild.id,'delivery_time','').strip()
        if not current_delivery:
            return await i.response.send_message('❌ 店長目前尚未設定統一交貨時間，請稍後再試。',ephemeral=True)
        await create_order_and_show_payment(i,self.p,self.qty,price(self.p),price(self.p)*self.qty,current_delivery)
        button.disabled=True
    @discord.ui.button(label='❌ 無法接受，取消',style=discord.ButtonStyle.danger)
    async def reject(self,i,button):
        await i.response.edit_message(content=template(i.guild.id,'cancel_text','❌ 已取消本次購買。'),embed=None,view=None)

async def has_payment_proof(ch, buyer_id, order_id=None):
    """付款確認前必須找到客人上傳的付款成功明細圖片。"""
    if not isinstance(ch,discord.TextChannel) or not buyer_id:
        return False
    # 先查資料庫已綁定的圖片，避免歷史訊息超過掃描範圍時漏判。
    if order_id is not None:
        stored=q('SELECT payment_proof_url,payment_proof_message_id FROM orders WHERE id=?',(order_id,),True)
        if stored and stored[0]['payment_proof_url']:
            return True
    image_exts=('.png','.jpg','.jpeg','.webp','.gif','.bmp','.heic','.heif')
    try:
        async for msg in ch.history(limit=500):
            if str(getattr(msg.author,'id','')) != str(buyer_id):
                continue
            for att in msg.attachments:
                ctype=(att.content_type or '').lower()
                filename=(getattr(att,'filename','') or '').lower()
                if ctype.startswith('image/') or filename.endswith(image_exts):
                    if order_id is not None:
                        q('UPDATE orders SET payment_proof_url=?,payment_proof_message_id=? WHERE id=?',
                          (att.url,str(msg.id),order_id))
                    return True
    except (discord.Forbidden,discord.HTTPException) as e:
        print(f'[PAYMENT_PROOF] history check failed｜channel={getattr(ch,"id",None)}｜error={e!r}')
    return False


class OrderManageView(discord.ui.View):
    def __init__(self,oid):
        super().__init__(timeout=None); self.oid=oid; self.build()
    def build(self):
        r=q('SELECT * FROM orders WHERE id=?',(self.oid,),True); status=r[0]['status'] if r else ''
        if status=='待確認付款':
            ch_id = r[0]['channel_id'] if r else ''
            ch = bot.get_channel(int(ch_id)) if str(ch_id).isdigit() else None
            proof_ready = bool(r and r[0]['payment_proof_url'])
            # 建立 View 時先看資料庫；若圖片尚未綁定，再嘗試掃描目前工單。
            if not proof_ready and isinstance(ch,discord.TextChannel) and r:
                # build() 不能 await；先不顯示確認按鈕，等 on_message 收到圖片後重新發送可確認的 View。
                proof_ready = False
            if proof_ready:
                b=discord.ui.Button(label='💰 確認付款',style=discord.ButtonStyle.success,custom_id=f'ct_confirm:{self.oid}',row=0); b.callback=self.confirm_payment; self.add_item(b)
        elif status=='待交貨':
            b=discord.ui.Button(label='🛠️ 開始處理',style=discord.ButtonStyle.primary,custom_id=f'ct_process:{self.oid}',row=0); b.callback=self.start; self.add_item(b)
        elif status=='處理中':
            b=discord.ui.Button(label='📦 完成交貨',style=discord.ButtonStyle.primary,custom_id=f'ct_deliver:{self.oid}',row=0); b.callback=self.deliver; self.add_item(b)
        elif status=='待收貨':
            b=discord.ui.Button(label='✅ 完成訂單',style=discord.ButtonStyle.success,custom_id=f'ct_finish:{self.oid}',row=0); b.callback=self.finish; self.add_item(b)
        r2=q('SELECT channel_id FROM orders WHERE id=?',(self.oid,),True); url=None
        if r2:
            ch=bot.get_channel(int(r2[0]['channel_id']))
            if isinstance(ch,discord.TextChannel): url=ch.jump_url
        if url: self.add_item(discord.ui.Button(label='🎫 前往工單',style=discord.ButtonStyle.link,url=url,row=0))
    async def check(self,i): return admin(i.user)
    async def confirm_payment(self,i):
        if not await self.check(i): return await deny(i)
        r=q('SELECT * FROM orders WHERE id=?',(self.oid,),True)
        if not r: return await i.response.send_message('❌ 找不到訂單。',ephemeral=True)
        o=r[0]
        if o['status']!='待確認付款': return await i.response.send_message(f'ℹ️ 目前狀態為「{o["status"]}」。',ephemeral=True)
        ch=i.guild.get_channel(int(o['channel_id']))
        if not await has_payment_proof(ch,o['buyer_id'],self.oid):
            return await i.response.send_message('❌ 尚未找到客人上傳的付款成功明細圖片，請先請客人上傳圖片後再確認付款。',ephemeral=True)
        if not await confirm_dialog(i,'確認這筆付款已收到嗎？','確認付款',self.oid): return
    async def start(self,i): await self.change(i,'處理中','開始處理')
    async def deliver(self,i): await self.change(i,'待收貨','完成交貨')
    async def change(self,i,new_status,action):
        if not await self.check(i): return await deny(i)
        await i.response.defer(ephemeral=True)
        r=q('SELECT * FROM orders WHERE id=?',(self.oid,),True)
        if not r: return await i.followup.send('❌ 找不到訂單。',ephemeral=True)
        o=r[0]; q('UPDATE orders SET status=? WHERE id=?',(new_status,self.oid)); log_order(self.oid,i.guild.id,i.user.id,action,f'{o["status"]} → {new_status}')
        await rename_order_channel(q('SELECT * FROM orders WHERE id=?',(self.oid,),True)[0],i.guild,action)
        rr=q('SELECT * FROM orders WHERE id=?',(self.oid,),True)[0]; await status_announce(rr,i.guild); await i.followup.send(f'✅ 訂單 #{o["ticket_no"]} 已更新為 **{new_status}**。',ephemeral=True)
    async def finish(self,i):
        if not await self.check(i): return await deny(i)
        await i.response.defer(ephemeral=True)
        r=q('SELECT * FROM orders WHERE id=?',(self.oid,),True)
        if not r: return await i.followup.send('❌ 找不到訂單。',ephemeral=True)
        o=r[0]
        if o['status']!='待收貨': return await i.followup.send(f'ℹ️ 目前狀態為「{o["status"]}」，無法直接完成訂單。',ephemeral=True)
        q('UPDATE orders SET status=?,completed_at=? WHERE id=?',('結單',now(),self.oid)); log_order(self.oid,i.guild.id,i.user.id,'完成訂單','管理員完成訂單')
        rr=q('SELECT * FROM orders WHERE id=?',(self.oid,),True)[0]
        await rename_order_channel(rr,i.guild,'完成訂單')
        ch=i.guild.get_channel(int(rr['channel_id']))
        if isinstance(ch,discord.TextChannel):
            await revoke_buyer_access(ch,rr['buyer_id'])
            await move_closed_ticket_to_service_category(ch,rr['service_type'] if 'service_type' in rr.keys() else '幣號')
        await status_announce(rr,i.guild); await i.followup.send(f'✅ 訂單 #{o["ticket_no"]} 已完成並結單，客人存取權已移除。',ephemeral=True)

class ConfirmView(discord.ui.View):
    def __init__(self,oid): super().__init__(timeout=60); self.oid=oid
    @discord.ui.button(label='✅ 確認',style=discord.ButtonStyle.success)
    async def yes(self,i,b):
        if not admin(i.user): return await deny(i)
        await i.response.defer(ephemeral=True)
        r=q('SELECT * FROM orders WHERE id=?',(self.oid,),True)
        if not r: return await i.followup.send('❌ 找不到訂單。',ephemeral=True)
        o=r[0]
        if o['status']!='待確認付款': return await i.followup.send('ℹ️ 這筆訂單已不是待確認付款。',ephemeral=True)
        ch=i.guild.get_channel(int(o['channel_id']))
        if not await has_payment_proof(ch,o['buyer_id'],self.oid):
            return await i.followup.send('❌ 尚未找到客人上傳的付款成功明細圖片，請先請客人上傳圖片後再確認付款。',ephemeral=True)
        new='待排單' if o['service_type']=='代肝' else '待交貨'
        q('UPDATE orders SET status=? WHERE id=?',(new,self.oid)); log_order(self.oid,i.guild.id,i.user.id,'確認付款',f'{o["status"]} → {new}')
        ch=i.guild.get_channel(int(o['channel_id']))
        if isinstance(ch,discord.TextChannel) and o['service_type']=='代肝':
            await ch.send('🎮 **請提供遊戲帳號**\n付款已確認，請直接在此工單提供您的遊戲帳號。\n📢 已通知店長，收到帳號後將安排排單。')
        rr=q('SELECT * FROM orders WHERE id=?',(self.oid,),True)[0]; await rename_order_channel(rr,i.guild,'店長確認付款後自動更新'); await status_announce(rr,i.guild)
        await i.edit_original_response(content=f'✅ 已確認付款｜#{o["ticket_no"]}',view=None)
    @discord.ui.button(label='取消',style=discord.ButtonStyle.secondary)
    async def no(self,i,b): await i.response.edit_message(content='已取消確認操作。',view=None)

async def confirm_dialog(i,text,title,oid):
    if not admin(i.user): return await deny(i)
    await i.response.send_message(f'💰 **{title}**\n{text}',view=ConfirmView(oid),ephemeral=True); return True

class PaymentMethodView(discord.ui.View):
    def __init__(self,oid):
        super().__init__(timeout=None); self.oid=oid
        # 每張訂單使用獨立 custom_id，避免多張付款面板互相吃到別張訂單的 callback。
        b=discord.ui.Button(label='🏪 無卡存款（帶紙鈔至 7-11）',style=discord.ButtonStyle.primary,custom_id=f'ct_pay_nocard:{oid}',row=0)
        b.callback=self.nocard; self.add_item(b)
        b2=discord.ui.Button(label='🏦 匯款（轉帳）',style=discord.ButtonStyle.primary,custom_id=f'ct_pay_transfer:{oid}',row=0)
        b2.callback=self.transfer; self.add_item(b2)
    async def choose(self,i,method):
        # 先確認 Interaction，再進行資料庫、狀態頻道與付款頁更新，避免超過 Discord 3 秒限制。
        try:
            await i.response.defer()
        except discord.InteractionResponded:
            pass
        r=q('SELECT * FROM orders WHERE id=?',(self.oid,),True)
        if not r or str(r[0]['buyer_id'])!=str(i.user.id): return await i.followup.send('❌ 這不是您的訂單，無法操作。',ephemeral=True)
        o=r[0]
        if o['status']!='待付款': return await i.followup.send('ℹ️ 這筆訂單目前無法選擇付款方式。',ephemeral=True)
        q('UPDATE orders SET payment_method=? WHERE id=?',(method,self.oid)); log_order(self.oid,o['guild_id'],i.user.id,'選擇付款方式',method)
        if method=='無卡存款':
            text=template(i.guild.id,'no_card_text','中信（7-11ATM）\n銀行帳戶：')
            if '{bank_info}' in text:
                text=text.replace('{bank_info}',payment_info(i.guild.id))
        else:
            text=template(i.guild.id,'transfer_text','🏦 **匯款（轉帳）**\n請依下方資訊完成轉帳：\n\n{bank_info}').format(bank_info=payment_info(i.guild.id))
        e=discord.Embed(title=f'💳 {method}',description=text)
        e.set_footer(text=template(i.guild.id,'payment_selected_text','完成付款後，請按下「💳 完成付款」通知店家。'))
        rr=q('SELECT * FROM orders WHERE id=?',(self.oid,),True)[0]
        await status_announce(rr,i.guild)
        await i.edit_original_response(embed=e,view=PaymentView(self.oid))
    async def nocard(self,i): await self.choose(i,'無卡存款')
    async def transfer(self,i): await self.choose(i,'匯款（轉帳）')

class PaymentView(discord.ui.View):
    def __init__(self,oid):
        super().__init__(timeout=None); self.oid=oid
        b=discord.ui.Button(label='💳 完成付款',style=discord.ButtonStyle.success,custom_id=f'ct_paid:{oid}'); b.callback=self.paid; self.add_item(b)
        b2=discord.ui.Button(label='❌ 取消訂單',style=discord.ButtonStyle.danger,custom_id=f'ct_cancel:{oid}'); b2.callback=self.cancel; self.add_item(b2)
    async def paid(self,i):
        try: await i.response.defer()
        except discord.InteractionResponded: pass
        r=q('SELECT * FROM orders WHERE id=?',(self.oid,),True)
        if not r or str(r[0]['buyer_id'])!=str(i.user.id): return await i.followup.send('❌ 這不是您的訂單，無法操作。',ephemeral=True)
        o=r[0]
        if o['status']!='待付款': return await i.followup.send('ℹ️ 這筆訂單已經提交過付款通知，請勿重複操作。',ephemeral=True)
        q('UPDATE orders SET status=?,paid_at=? WHERE id=?',('待確認付款',now(),self.oid)); log_order(self.oid,o['guild_id'],i.user.id,'完成付款',f'付款方式：{o["payment_method"] or "未選擇"}')
        guild=i.guild; updated=q('SELECT * FROM orders WHERE id=?',(self.oid,),True)[0]
        # 支援「先上傳付款圖片、再按完成付款」：狀態切成待確認付款後立即回掃歷史並綁定圖片。
        proof_found=await has_payment_proof(i.channel,o['buyer_id'],self.oid)
        updated=q('SELECT * FROM orders WHERE id=?',(self.oid,),True)[0]
        await rename_order_channel(updated,guild,'客人完成付款後自動更新工單名稱'); await status_announce(updated,guild)
        if proof_found:
            await i.channel.send(f'💳 **付款通知**\n已收到 {i.user.mention} 的付款通知。\n\n🧾 **已找到付款成功明細圖片**，已綁定至訂單 #{updated["ticket_no"]}。\n👑 店長現在可以按「💰 確認付款」。', view=OrderManageView(self.oid))
        else:
            await i.channel.send(f'💳 **付款通知**\n已收到 {i.user.mention} 的付款通知。\n\n🧾 請在此工單上傳**付款成功明細圖片**。\n如果沒有附上圖片，店長會提醒您補上。')
        await i.followup.send(template(i.guild.id,'paid_text','✅ **成功提交付款通知，請等待店長**'),ephemeral=True)
    async def cancel(self,i):
        try: await i.response.defer()
        except discord.InteractionResponded: pass
        r=q('SELECT * FROM orders WHERE id=?',(self.oid,),True)
        if not r or str(r[0]['buyer_id'])!=str(i.user.id): return await i.followup.send('❌ 這不是您的訂單，無法操作。',ephemeral=True)
        if r[0]['status']!='待付款': return await i.followup.send('❌ 這筆訂單已進入付款流程，目前無法取消。',ephemeral=True)
        with db:
            cur=db.cursor(); cur.execute('UPDATE orders SET status=? WHERE id=? AND status=?',('已取消',self.oid,'待付款'))
            cur.execute('SELECT product,quantity,stock_reserved FROM orders WHERE id=?',(self.oid,)); rr=cur.fetchone()
            if rr and int(rr['stock_reserved'] or 0)==1:
                cur.execute('UPDATE products SET stock=stock+? WHERE product=?',(int(rr['quantity']),rr['product']))
                cur.execute('UPDATE orders SET stock_reserved=0 WHERE id=?',(self.oid,))
            db.commit()
        updated=q('SELECT * FROM orders WHERE id=?',(self.oid,),True)[0]; await rename_order_channel(updated,i.guild,'客人取消訂單'); await i.followup.send('❌ 訂單已取消。'); await status_announce(updated,i.guild)


@bot.tree.command(name='設定代肝價格',description='設定代肝額度價格')
@app_commands.describe(額度='例如 300M',價格='價格（NT）')
@app_commands.choices(額度=choices(BOOST_TIERS))
async def set_boost_price(i,額度:str,價格:int):
    if not admin(i.user): return await deny(i)
    if 價格<0: return await i.response.send_message('❌ 價格不能小於 0。',ephemeral=True)
    q('INSERT INTO boost_prices(guild_id,tier,price) VALUES(?,?,?) ON CONFLICT(guild_id,tier) DO UPDATE SET price=excluded.price',(str(i.guild.id),額度,價格)); await i.response.send_message(f'✅ {額度} 代肝價格已設定為 NT${價格:,}。',ephemeral=True)

@bot.tree.command(name='代肝價格表',description='查看代肝價格')
async def boost_prices(i):
    if not admin(i.user): return await deny(i)
    await i.response.send_message('🛠️ **代肝價格**\n'+'\n'.join(f'{t}：NT${boost_price(i.guild.id,t):,}' if boost_price(i.guild.id,t)>0 else f'{t}：尚未設定' for t in BOOST_TIERS),ephemeral=True)

@bot.tree.command(name='設定幣號狀態',description='設定幣號規格是否正常提供')
@app_commands.describe(商品='幣號規格',狀態='正常提供／暫停提供／缺貨')
@app_commands.choices(商品=choices(COIN_PRODUCTS),狀態=choices(AVAILABILITY))
async def set_coin_status(i,商品:str,狀態:str):
    if not admin(i.user): return await deny(i)
    q('INSERT OR IGNORE INTO products(product,price,stock,enabled,availability_status) VALUES(?,?,?,?,?)',(商品,0,0,1,狀態)); q('UPDATE products SET availability_status=? WHERE product=?',(狀態,商品)); await i.response.send_message(f'✅ {商品} 狀態已設定為 **{狀態}**。',ephemeral=True)

@bot.tree.command(name='設定價目表頻道',description='設定幣號與代肝價目表頻道')
@app_commands.describe(類型='價目表類型',頻道='頻道')
@app_commands.choices(類型=choices(['幣號價目表','代肝價目表']))
async def set_price_channel(i,類型:str,頻道:discord.TextChannel):
    if not admin(i.user): return await deny(i)
    setv(i.guild.id,'coin_price_channel_id' if 類型=='幣號價目表' else 'boost_price_channel_id',頻道.id); await i.response.send_message(f'✅ {類型}已設定為 {頻道.mention}',ephemeral=True)

@bot.tree.command(name='設定非營業時間',description='開啟非營業時間模式')
async def set_off_hours(i):
    if not admin(i.user): return await deny(i)
    setv(i.guild.id,'off_hours','1'); await i.response.send_message('🕐 已開啟非營業時間。客人仍可正常下單及付款，付款後等待店長回來確認。',ephemeral=True)

@bot.tree.command(name='關閉非營業時間',description='關閉非營業時間模式')
async def close_off_hours(i):
    if not admin(i.user): return await deny(i)
    setv(i.guild.id,'off_hours','0'); await i.response.send_message('🟢 已關閉非營業時間模式。',ephemeral=True)

@bot.tree.command(name='關閉自動算價',description='關閉店長在聊天室輸入額度×數量時的自動算價')
async def disable_auto_price(i):
    if not admin(i.user): return await deny(i)
    setv(i.guild.id,'auto_price_calc','0')
    await i.response.send_message('⏸️ 已關閉聊天室自動算價。之後輸入 50*3 這類內容，機器人不會自動回覆價格。',ephemeral=True)

@bot.tree.command(name='開啟自動算價',description='開啟店長在聊天室輸入額度×數量時的自動算價')
async def enable_auto_price(i):
    if not admin(i.user): return await deny(i)
    setv(i.guild.id,'auto_price_calc','1')
    await i.response.send_message('▶️ 已開啟聊天室自動算價。',ephemeral=True)

# ---- 管理指令 ----
async def deny(i):
    text='❌ 你沒有管理權限。\n請確認你有「管理伺服器／管理頻道」權限，或已被設定為 CT小舖 管理員。'
    if i.response.is_done():
        return await i.followup.send(text,ephemeral=True)
    return await i.response.send_message(text,ephemeral=True)

@bot.tree.command(name='價格表',description='查看目前商品價格')
async def prices(i):
    if not admin(i.user): return await deny(i)
    rows=q("SELECT product,price FROM products ORDER BY CASE product WHEN '50M' THEN 1 WHEN '100M' THEN 2 ELSE 3 END",(),True)
    await i.response.send_message('💰 **目前價格**\n'+'\n'.join(f'{r["product"]}：NT${r["price"]:,}' for r in rows),ephemeral=True)

@bot.tree.command(name='設定價格',description='設定商品單價')
@app_commands.describe(商品='商品',價格='每隻單價（NT）')
@app_commands.choices(商品=choices(COIN_PRODUCTS))
async def set_price(i,商品:str,價格:int):
    if not admin(i.user): return await deny(i)
    if 價格<0: return await i.response.send_message('❌ 價格不能小於 0。',ephemeral=True)
    q('UPDATE products SET price=? WHERE product=?',(價格,商品)); await i.response.send_message(f'✅ {商品} 單價已設定為 NT${價格:,}。',ephemeral=True)

@bot.tree.command(name='設定最低購買數量',description='設定單一幣號規格的最低購買數量')
@app_commands.describe(商品='幣號規格',數量='最低購買隻數，0 代表不限')
@app_commands.choices(商品=choices(COIN_PRODUCTS))
async def set_min_purchase_qty(i,商品:str,數量:int):
    if not admin(i.user): return await deny(i)
    if 數量<0 or 數量>999999: return await i.response.send_message('❌ 最低購買數量請輸入 0～999999。',ephemeral=True)
    setv(i.guild.id,f'min_purchase_qty:{商品}',數量)
    await i.response.send_message(f'✅ {商品} 最低購買數量已設定為 **{數量} 隻**。'+('（不限）' if 數量==0 else ''),ephemeral=True)

@bot.tree.command(name='設定庫存',description='設定目前現貨數量')
@app_commands.describe(商品='商品',數量='目前現貨隻數')
@app_commands.choices(商品=choices(COIN_PRODUCTS))
async def set_stock(i,商品:str,數量:int):
    if not admin(i.user): return await deny(i)
    if 數量<0: return await i.response.send_message('❌ 庫存不能小於 0。',ephemeral=True)
    q('UPDATE products SET stock=? WHERE product=?',(數量,商品)); await i.response.send_message(f'📦 {商品} 現貨已設定為 **{數量} 隻**。',ephemeral=True)

@bot.tree.command(name='庫存',description='查看目前現貨')
async def stocks(i):
    if not admin(i.user): return await deny(i)
    rows=q("SELECT product,stock FROM products ORDER BY CASE product WHEN '50M' THEN 1 WHEN '100M' THEN 2 ELSE 3 END",(),True)
    await i.response.send_message('📦 **目前現貨**\n'+'\n'.join(f'{r["product"]}：{r["stock"]} 隻' for r in rows),ephemeral=True)

@bot.tree.command(name='設定補貨',description='設定商品下一次補貨時間')
@app_commands.describe(商品='商品',時間='例如：9/6 20:00')
@app_commands.choices(商品=choices(COIN_PRODUCTS))
async def set_restock(i,商品:str,時間:str):
    if not admin(i.user): return await deny(i)
    setv(i.guild.id,f'restock_{商品}',時間); await i.response.send_message(f'📦 {商品} 下一次補貨時間已設定：**{時間}**',ephemeral=True)

@bot.tree.command(name='店長狀態',description='設定店長營業或休息')
@app_commands.describe(狀態='營業中／休息中')
@app_commands.choices(狀態=choices(['營業中','休息中']))
async def shop_status(i,狀態:str):
    if not admin(i.user): return await deny(i)
    setv(i.guild.id,'shop_status',狀態); await i.response.send_message(f'✅ 店長狀態：**{狀態}**。新工單會自動顯示。',ephemeral=True)

@bot.tree.command(name='暫停接單',description='暫停客人下單')
async def pause(i):
    if not admin(i.user): return await deny(i)
    setv(i.guild.id,'shop_status','暫停接單'); await i.response.send_message('⏸️ 已暫停接單。客人仍可開單，但無法建立付款訂單。',ephemeral=True)

@bot.tree.command(name='恢復接單',description='恢復客人下單')
async def resume(i):
    if not admin(i.user): return await deny(i)
    setv(i.guild.id,'shop_status','營業中'); await i.response.send_message('▶️ 已恢復接單。',ephemeral=True)

@bot.tree.command(name='付款資訊',description='查看目前付款資訊')
async def payinfo(i):
    if not admin(i.user): return await deny(i)
    await i.response.send_message(f'💳 **目前付款資訊**\n{payment_info(i.guild.id)}',ephemeral=True)

@bot.tree.command(name='設定付款',description='設定付款資訊')
@app_commands.describe(銀行='銀行名稱',代碼='銀行代碼',帳號='收款帳號',戶名='戶名')
async def setpay(i,銀行:str='',代碼:str='',帳號:str='',戶名:str=''):
    if not admin(i.user): return await deny(i)
    # 只更新有填的欄位；沒填的保留原值（以前只填一欄會把其他欄位清空）。
    upd=[(k,v) for k,v in [('pay_bank',銀行),('pay_code',代碼),('pay_account',帳號),('pay_name',戶名)] if v!='']
    if not upd: return await i.response.send_message('❌ 請至少填一個欄位。',ephemeral=True)
    for k,v in upd: setv(i.guild.id,k,v)
    await i.response.send_message('✅ 付款資訊已更新。\n'+payment_info(i.guild.id),ephemeral=True)

@bot.tree.command(name='設定交貨時間',description='設定本店今日統一交貨時間')
@app_commands.describe(時間='例如：9/6 21:30')
async def set_delivery(i,時間:str):
    if not admin(i.user): return await deny(i)
    setv(i.guild.id,'delivery_time',時間); await i.response.send_message(f'🚚 本店今日統一交貨時間已設定為：**{時間}**',ephemeral=True)

@bot.tree.command(name='設定付款提醒',description='設定待付款訂單多久後提醒一次')
@app_commands.describe(分鐘='例如 15，填 0 可關閉自動提醒')
async def set_payment_reminder(i,分鐘:int):
    if not admin(i.user): return await deny(i)
    if 分鐘<0 or 分鐘>10080: return await i.response.send_message('❌ 分鐘請輸入 0～10080。',ephemeral=True)
    setv(i.guild.id,'payment_reminder_minutes',分鐘)
    await i.response.send_message('🔔 已關閉付款提醒。' if 分鐘==0 else f'🔔 已設定待付款 **{分鐘} 分鐘**後提醒一次。',ephemeral=True)

@bot.tree.command(name='設定付款方式',description='設定付款方式頁面與各付款方式說明')
@app_commands.describe(類型='要修改的付款訊息',內容='新的訊息內容')
@app_commands.choices(類型=choices(['付款方式選擇','無卡存款說明','匯款說明','選擇付款後提示','完成付款提示']))
async def set_payment_method(i,類型:str,內容:str):
    if not admin(i.user): return await deny(i)
    mp={'付款方式選擇':'pay_method_intro','無卡存款說明':'no_card_text','匯款說明':'transfer_text','選擇付款後提示':'payment_selected_text','完成付款提示':'paid_text'}
    setv(i.guild.id,mp[類型],內容); await i.response.send_message(f'✅「{類型}」已更新。',ephemeral=True)

@bot.tree.command(name='設定訂單紀錄',description='選擇訂單紀錄頻道')
@app_commands.describe(頻道='請選擇頻道')
async def set_order_log(i,頻道:discord.TextChannel):
    if not admin(i.user): return await deny(i)
    setv(i.guild.id,'order_log_channel_id',頻道.id); await i.response.send_message(f'✅ 訂單紀錄頻道已設定為 {頻道.mention}',ephemeral=True)

@bot.tree.command(name='設定操作紀錄',description='選擇管理員操作紀錄頻道')
@app_commands.describe(頻道='請選擇頻道')
async def set_action_log(i,頻道:discord.TextChannel):
    if not admin(i.user): return await deny(i)
    setv(i.guild.id,'action_log_channel_id',頻道.id); await i.response.send_message(f'✅ 操作紀錄頻道已設定為 {頻道.mention}',ephemeral=True)

@bot.tree.command(name='設定狀態頻道',description='選擇工單狀態公告頻道')
@app_commands.describe(頻道='請選擇頻道')
async def set_status_channel(i,頻道:discord.TextChannel):
    if not admin(i.user): return await deny(i)
    setv(i.guild.id,'status_channel_id',頻道.id); await i.response.send_message(f'✅ 工單狀態公告頻道已設定為 {頻道.mention}',ephemeral=True)

@bot.tree.command(name='設定訊息',description='設定客人看到的 Bot 訊息')
@app_commands.describe(類型='要修改的訊息',內容='新的訊息內容')
@app_commands.choices(類型=choices(['初始面板','沒貨提示','數量不足提示','付款完成提示','取消提示','暫停接單提示','狀態公告格式','訂單確認頁','缺貨交貨時間確認頁']))
async def setmsg(i,類型:str,內容:str):
    if not admin(i.user): return await deny(i)
    mp={'初始面板':'panel_text','沒貨提示':'out_of_stock','數量不足提示':'not_enough_stock','付款完成提示':'paid_text','取消提示':'cancel_text','暫停接單提示':'paused_text','狀態公告格式':'status_template','訂單確認頁':'order_confirm_text','缺貨交貨時間確認頁':'delivery_accept_text'}
    setv(i.guild.id,mp[類型],內容); await i.response.send_message(f'✅「{類型}」已更新。',ephemeral=True)


# ===== V114：補回 V111「全面清理」時誤刪、但仍被呼叫的函式 =====
async def _safe_edit_channel(ch, timeout=20, **kwargs):
    """channel.edit 加上逾時。Discord 對頻道改名有頻率限制（約 10 分鐘 2 次），
    被限速時 discord.py 會一直等待，讓指令卡死；這裡逾時就放棄並回傳 False。"""
    try:
        await asyncio.wait_for(ch.edit(**kwargs), timeout=timeout)
        return True
    except asyncio.TimeoutError:
        print(f'[CHANNEL_EDIT] 逾時（可能被 Discord 限速）｜channel={getattr(ch,"id",None)}｜fields={list(kwargs)}')
    except (discord.Forbidden, discord.HTTPException) as e:
        print(f'[CHANNEL_EDIT] 失敗｜channel={getattr(ch,"id",None)}｜fields={list(kwargs)}｜error={e!r}')
    return False

async def allocate_ticket_no(guild_id):
    """配發下一個工單號（4 位數字字串），同時避開資料庫中已存在的最大工單號。"""
    gid = str(guild_id)
    async with lock:
        with db:
            cur = db.cursor()
            cur.execute('SELECT next_no FROM ticket_counters WHERE guild_id=?', (gid,))
            row = cur.fetchone()
            n = int(row['next_no']) if row else 1
            for sql in ('SELECT MAX(CAST(ticket_no AS INTEGER)) AS m FROM ticket_channels WHERE guild_id=?',
                        'SELECT MAX(CAST(ticket_no AS INTEGER)) AS m FROM orders WHERE guild_id=?'):
                cur.execute(sql, (gid,))
                r = cur.fetchone()
                if r and r['m'] is not None:
                    n = max(n, int(r['m']) + 1)
            cur.execute('INSERT INTO ticket_counters(guild_id,next_no) VALUES(?,?) ON CONFLICT(guild_id) DO UPDATE SET next_no=excluded.next_no', (gid, n + 1))
    return f'{n:04d}'

async def get_ticket_creation_category(guild):
    """/設定開單分類 設定的新工單分類；沒設定或已被刪除時回傳 None。"""
    cid = str(get(guild.id, 'ticket_category_id', '') or '')
    if cid.isdigit():
        cat = guild.get_channel(int(cid))
        if isinstance(cat, discord.CategoryChannel):
            return cat
    return None

async def ensure_service_category(guild, service):
    """取得「幣號／代肝」結單分類；尚未設定或已被刪除時回傳 None。"""
    key_ = 'service_category_boost_id' if str(service) == '代肝' else 'service_category_coin_id'
    cid = str(get(guild.id, key_, '') or '')
    if cid.isdigit():
        cat = guild.get_channel(int(cid))
        if isinstance(cat, discord.CategoryChannel):
            return cat
    return None

async def move_closed_ticket_to_service_category(ch, service_type='幣號'):
    """把結單的工單移入對應服務的結單分類。成功（或本來就在裡面）回傳 True。"""
    if not isinstance(ch, discord.TextChannel):
        return False
    service = '代肝' if str(service_type) == '代肝' else '幣號'
    cat = await ensure_service_category(ch.guild, service)
    if not isinstance(cat, discord.CategoryChannel):
        print(f'[MOVE_CLOSED] 尚未設定「{service}」結單分類｜guild={ch.guild.id}｜請用 /建立服務工單分類 或 /設定服務工單分類')
        return False
    if ch.category_id == cat.id:
        return True
    if len(cat.channels) >= 50:
        print(f'[MOVE_CLOSED] 結單分類已滿 50 個頻道｜category={cat.id}')
        return False
    return await _safe_edit_channel(ch, timeout=15, category=cat, reason='CT小舖 結單移入服務分類')

def _channel_ticket_number(ch):
    if not isinstance(ch, discord.TextChannel):
        return None
    return _resolve_ticket_number(ch)

def _auto_category_config(guild_id):
    raw = str(get(guild_id, 'auto_category_source_ids', '') or '')
    dest = str(get(guild_id, 'auto_category_destination_id', '') or '')
    mode = get(guild_id, 'auto_category_sort_mode', AUTO_CATEGORY_MODES[0])
    if mode not in AUTO_CATEGORY_MODES:
        mode = AUTO_CATEGORY_MODES[0]
    return {
        'enabled': get(guild_id, 'auto_category_enabled', '0') == '1',
        'source_ids': [int(x) for x in re.findall(r'\d+', raw)],
        'destination_id': int(dest) if dest.isdigit() else None,
        'sort_mode': mode,
    }

async def auto_categorize_channels(guild, source_ids, destination_id, sort_mode):
    """把來源分類內的文字頻道移到目標分類，並依 sort_mode 排序目標分類。回傳 (已移動, 失敗, 錯誤訊息)。"""
    dest = guild.get_channel(int(destination_id)) if destination_id else None
    if not isinstance(dest, discord.CategoryChannel):
        return 0, 0, '目標分類不存在或已被刪除，請重新使用 `/設定自動分類`。'
    sources = [guild.get_channel(int(x)) for x in (source_ids or [])]
    sources = [c for c in sources if isinstance(c, discord.CategoryChannel) and c.id != dest.id]
    to_move = [c for cat in sources for c in cat.text_channels]
    room = max(0, 50 - len(dest.channels))
    failed = max(0, len(to_move) - room)
    to_move = to_move[:room]
    pool = list(dest.text_channels) + to_move
    if not pool:
        return 0, failed, None

    def num(ch):
        n = _resolve_ticket_number(ch)
        return int(n) if n is not None else None
    mode = sort_mode if sort_mode in AUTO_CATEGORY_MODES else AUTO_CATEGORY_MODES[0]
    if mode == '工單號小-大':
        ordered = sorted(pool, key=lambda c: (num(c) is None, num(c) or 0, c.position, c.id))
    elif mode == '工單號大-小':
        ordered = sorted(pool, key=lambda c: (num(c) is None, -(num(c) or 0), c.position, c.id))
    elif mode == '隨機':
        ordered = pool[:]; random.shuffle(ordered)
    else:
        ordered = pool[:]  # 按照目前排序狀況：原有順序不動，新移入的接在後面

    moving_ids = {c.id for c in to_move}
    payload = []
    for pos, c in enumerate(ordered):
        item = {'id': c.id, 'position': pos}
        if c.id in moving_ids:
            item['parent_id'] = dest.id
            item['lock_permissions'] = False
        payload.append(item)
    try:
        await bot.http.bulk_channel_update(guild.id, payload, reason='CT小舖 自動分類')
    except (AttributeError, TypeError):
        # discord.py 版本沒有 bulk API 時，逐一處理。
        ok_count = 0
        for pos, c in enumerate(ordered):
            kw = {'position': pos}
            if c.id in moving_ids: kw['category'] = dest
            if await _safe_edit_channel(c, timeout=15, reason='CT小舖 自動分類', **kw):
                if c.id in moving_ids: ok_count += 1
            elif c.id in moving_ids:
                failed += 1
            await asyncio.sleep(0.3)
        return ok_count, failed, None
    except (discord.Forbidden, discord.HTTPException) as e:
        return 0, failed + len(to_move), f'Discord 拒絕操作：{e}'
    return len(to_move), failed, None

class AutoCategorySetupView(discord.ui.View):
    """/設定自動分類：選來源分類、目標分類、排序方式，按儲存後開啟自動分類。"""
    def __init__(self, guild, operator_id=None):
        super().__init__(timeout=600)
        self.guild = guild
        self.operator_id = operator_id
        cfg = _auto_category_config(guild.id)
        self.source_ids = list(cfg['source_ids'])
        self.destination_id = cfg['destination_id']
        self.sort_mode = cfg['sort_mode']

        self.source_select = discord.ui.ChannelSelect(placeholder='① 選擇來源分類（可複選）', channel_types=[discord.ChannelType.category], min_values=1, max_values=10, row=0)
        self.source_select.callback = self.on_source
        self.add_item(self.source_select)
        self.dest_select = discord.ui.ChannelSelect(placeholder='② 選擇目標分類', channel_types=[discord.ChannelType.category], min_values=1, max_values=1, row=1)
        self.dest_select.callback = self.on_dest
        self.add_item(self.dest_select)
        self.mode_select = discord.ui.Select(placeholder='③ 選擇排序方式', options=[discord.SelectOption(label=m, value=m, default=(m == self.sort_mode)) for m in AUTO_CATEGORY_MODES], min_values=1, max_values=1, row=2)
        self.mode_select.callback = self.on_mode
        self.add_item(self.mode_select)
        self.save_button = discord.ui.Button(label='💾 儲存並開啟自動分類', style=discord.ButtonStyle.success, row=3)
        self.save_button.callback = self.on_save
        self.add_item(self.save_button)

    def _name(self, guild, cid):
        ch = guild.get_channel(int(cid)) if cid else None
        return ch.name if ch else f'（找不到 {cid}）'

    def _render(self, guild=None):
        guild = guild or self.guild
        src = '、'.join(f'**{self._name(guild, x)}**' for x in self.source_ids) or '尚未選擇'
        dst = f'**{self._name(guild, self.destination_id)}**' if self.destination_id else '尚未選擇'
        return ('🗂️ **自動分類設定**\n'
                f'① 來源分類：{src}\n② 目標分類：{dst}\n③ 排序方式：**{self.sort_mode}**\n\n'
                '選好後按「💾 儲存並開啟自動分類」。已存在的頻道可用 `/立即整理分類` 手動整理。')

    def _status(self):
        cfg = _auto_category_config(self.guild.id)
        return f'\n\n目前總開關：{"🟢 開啟" if cfg["enabled"] else "🔴 關閉"}'

    async def interaction_check(self, i):
        if not admin(i.user) or (self.operator_id and i.user.id != self.operator_id):
            await i.response.send_message('❌ 只有開啟這個面板的管理員可以操作。', ephemeral=True)
            return False
        return True

    async def _refresh(self, i):
        await i.response.edit_message(content=self._render(i.guild) + self._status(), view=self)

    async def on_source(self, i):
        self.source_ids = [c.id for c in self.source_select.values]
        await self._refresh(i)

    async def on_dest(self, i):
        vals = self.dest_select.values
        self.destination_id = vals[0].id if vals else None
        await self._refresh(i)

    async def on_mode(self, i):
        self.sort_mode = self.mode_select.values[0]
        for o in self.mode_select.options:
            o.default = (o.value == self.sort_mode)
        await self._refresh(i)

    async def on_save(self, i):
        if not self.source_ids or not self.destination_id:
            return await i.response.send_message('❌ 請先選擇來源分類與目標分類。', ephemeral=True)
        if self.destination_id in self.source_ids:
            return await i.response.send_message('❌ 目標分類不能同時是來源分類。', ephemeral=True)
        gid = i.guild.id
        setv(gid, 'auto_category_source_ids', ','.join(str(x) for x in self.source_ids))
        setv(gid, 'auto_category_destination_id', self.destination_id)
        setv(gid, 'auto_category_sort_mode', self.sort_mode)
        setv(gid, 'auto_category_enabled', '1')
        await i.response.edit_message(content='✅ **自動分類已儲存並開啟**\n' + self._render(i.guild).split('\n\n')[0].split('\n', 1)[1] + '\n\n新建立在來源分類的頻道會自動移到目標分類；現有頻道請用 `/立即整理分類`。', view=None)

async def _auto_category_on_create(ch):
    """新頻道建立在來源分類時，自動依設定整理（只在自動分類開啟時）。"""
    try:
        cfg = _auto_category_config(ch.guild.id)
        if not cfg['enabled'] or not cfg['destination_id'] or ch.category_id not in cfg['source_ids']:
            return
        lk = AUTO_CATEGORY_LOCKS.setdefault(ch.guild.id, asyncio.Lock())
        async with lk:
            await asyncio.sleep(3)
            moved, failed, err = await auto_categorize_channels(ch.guild, cfg['source_ids'], cfg['destination_id'], cfg['sort_mode'])
            if err: print(f'[AUTO_CATEGORY] 自動整理失敗｜guild={ch.guild.id}｜{err}')
    except Exception as e:
        print(f'[AUTO_CATEGORY] 例外｜error={e!r}'); traceback.print_exc()

def _replace_ticket_number_in_name(name, new_no):
    """只替換名稱中的工單號那一段，保留其他格式。找不到回傳 None。"""
    n = name or ''
    m = re.search(r'(?<=[-_ #])(\d{1,8})(?![\dmMｍ]|\s*隻)', n)
    if not m:
        return None
    return n[:m.start(1)] + new_no + n[m.end(1):]

async def renumber_all_tickets(guild):
    """依原工單號由小到大，把所有工單重新連號為 0001、0002…；同步改頻道名稱與資料庫。"""
    async with RENUMBER_LOCK:
        items = []
        for ch in guild.text_channels:
            try:
                if not is_ticket_candidate(ch): continue
                no = _resolve_ticket_number(ch)
            except Exception as e:
                print(f'[RENUMBER] 辨識失敗｜channel={ch.id}｜error={e!r}'); continue
            if no is None: continue
            items.append((int(no), ch.id, no, ch))
        items.sort(key=lambda x: (x[0], x[1]))
        mapping = []; changed = 0; failed = 0
        for idx, (_, _, old_no, ch) in enumerate(items, 1):
            new_no = f'{idx:04d}'
            old_fmt = f'{int(old_no):04d}'
            new_name = _replace_ticket_number_in_name(ch.name, new_no)
            if new_name is None:
                rows = q('SELECT * FROM orders WHERE channel_id=? ORDER BY id DESC LIMIT 1', (str(ch.id),), True)
                if rows:
                    o = rows[0]
                    new_name = rename_name(o['status'], new_no, o['product'], o['quantity'], service_type=o['service_type'] or '幣號')
                else:
                    new_name = f'工單-{new_no}'
            if new_name != ch.name:
                if not await _safe_edit_channel(ch, timeout=20, name=new_name, reason='CT小舖 自動排序工單號'):
                    failed += 1
                    continue
                await asyncio.sleep(0.4)
            try:
                q('UPDATE orders SET ticket_no=? WHERE channel_id=?', (new_no, str(ch.id)))
                remember(ch, new_no)
            except sqlite3.Error as e:
                print(f'[RENUMBER] DB 更新失敗｜channel={ch.id}｜error={e!r}')
                failed += 1
                continue
            mapping.append((old_fmt, new_no, ch))
            if old_fmt != new_no: changed += 1
        # 計數器跟著更新，避免之後新開單撞號。
        try:
            gid = str(guild.id); top = len(items)
            for sql in ('SELECT MAX(CAST(ticket_no AS INTEGER)) AS m FROM ticket_channels WHERE guild_id=?',
                        'SELECT MAX(CAST(ticket_no AS INTEGER)) AS m FROM orders WHERE guild_id=?'):
                r = q(sql, (gid,), True)
                if r and r[0]['m'] is not None: top = max(top, int(r[0]['m']))
            q('INSERT INTO ticket_counters(guild_id,next_no) VALUES(?,?) ON CONFLICT(guild_id) DO UPDATE SET next_no=excluded.next_no', (gid, top + 1))
        except sqlite3.Error as e:
            print(f'[RENUMBER] 計數器更新失敗｜error={e!r}')
        return {'found': len(items), 'changed': changed, 'failed': failed, 'mapping': mapping}

# === /改名工單：管理員快速改名；幣號數量使用與客人購買相同的原生 Modal ===
RENAME_COIN_STATUSES = ('待付款', '待確認付款', '待交貨', '待處理', '結單')
RENAME_BOOST_STATUSES = ('待付款', '待確認付款', '待倒', '待處理', '結單')
RENAME_COIN_AMOUNTS = tuple(f'{n}M' for n in range(50, 1001, 50))
RENAME_BOOST_AMOUNTS = tuple(f'{n}M' for n in range(50, 1001, 50))

async def _rename_service_autocomplete(i: discord.Interaction, current: str):
    vals=('幣號','代肝'); cur=(current or '').strip()
    return [app_commands.Choice(name=v,value=v) for v in vals if not cur or cur in v][:25]

async def _rename_status_autocomplete(i: discord.Interaction, current: str):
    service=str(getattr(i.namespace,'服務','') or '').strip()
    vals=RENAME_BOOST_STATUSES if service=='代肝' else RENAME_COIN_STATUSES
    cur=(current or '').strip()
    return [app_commands.Choice(name=v,value=v) for v in vals if not cur or cur in v][:25]

async def _rename_amount_autocomplete(i: discord.Interaction, current: str):
    service=str(getattr(i.namespace,'服務','') or '').strip()
    vals=RENAME_BOOST_AMOUNTS if service=='代肝' else RENAME_COIN_AMOUNTS
    cur=(current or '').strip().upper().replace('Ｍ','M')
    return [app_commands.Choice(name=v,value=v) for v in vals if not cur or cur in v.upper()][:25]

def _resolve_ticket_number(ch):
    no=ticket_no(ch.name) if isinstance(ch,discord.TextChannel) else None
    if no is None and isinstance(ch,discord.TextChannel):
        rows=q('SELECT ticket_no FROM ticket_channels WHERE channel_id=? LIMIT 1',(str(ch.id),),True)
        if rows: no=str(rows[0]['ticket_no'])
    if no is None and isinstance(ch,discord.TextChannel):
        rows=q('SELECT ticket_no FROM orders WHERE channel_id=? ORDER BY id DESC LIMIT 1',(str(ch.id),),True)
        if rows: no=str(rows[0]['ticket_no'])
    return f'{int(no):04d}' if no is not None and str(no).isdigit() else None

async def _complete_manual_rename(i, service, status, amount, qty):
    # 呼叫前必須已 defer；這裡一律用 followup 回覆，避免互動逾時（Discord 3 秒限制）。
    async def reply(msg):
        return await i.followup.send(msg,ephemeral=True)
    ch=i.channel
    if not isinstance(ch,discord.TextChannel):
        return await reply('❌ 請在工單頻道內使用。')
    no=_resolve_ticket_number(ch)
    if not no:
        return await reply('❌ 無法自動辨識這張工單的工單號，未執行改名。')
    rows=q('SELECT * FROM orders WHERE channel_id=? ORDER BY id DESC LIMIT 1',(str(ch.id),),True)
    order=rows[0] if rows else None
    old_status=str(order['status']) if order else (_status_from_name(ch.name) or '')
    old_service=str(order['service_type']) if order and order['service_type'] else (_service_from_name(ch.name) or service)
    new_name=rename_name(status,no,amount,qty,service_type=service)

    old_values=None
    if order:
        old_values=(order['ticket_no'],order['service_type'],order['status'],order['product'],order['quantity'],order['completed_at'])
        try:
            q('UPDATE orders SET ticket_no=?,service_type=?,status=?,product=?,quantity=?,completed_at=? WHERE id=?',
              (no,service,status,amount,qty,now() if status=='結單' else None,order['id']))
        except sqlite3.Error as e:
            return await reply(f'❌ 工單資料更新失敗，未改名：`{str(e)[:180]}`')

    def rollback():
        if order and old_values is not None:
            try:
                q('UPDATE orders SET ticket_no=?,service_type=?,status=?,product=?,quantity=?,completed_at=? WHERE id=?',
                  (*old_values,order['id']))
            except sqlite3.Error as rb:
                print(f'[RENAME_TICKET] DB rollback failed｜channel={ch.id}｜error={rb!r}')

    # Discord 對「頻道改名」有頻率限制（約 10 分鐘 2 次），超過時 discord.py 會一直等待，
    # 因此加上逾時，避免指令卡死。
    try:
        if ch.name!=new_name:
            await asyncio.wait_for(ch.edit(name=new_name,reason=f'CT小舖 /改名工單｜操作者 {i.user.id}'),timeout=20)
    except asyncio.TimeoutError:
        rollback()
        return await reply('⏳ Discord 頻道改名被限速（同一頻道約 10 分鐘內只能改名 2 次），未改名、資料已回復。請稍後再試。')
    except (discord.Forbidden,discord.HTTPException) as e:
        rollback()
        return await reply(f'❌ 工單改名失敗，資料已回復：`{str(e)[:180]}`')
    except Exception as e:
        import traceback; traceback.print_exc()
        rollback()
        return await reply(f'❌ 改名發生未預期錯誤，資料已回復：`{str(e)[:180]}`')

    warn=''
    try:
        remember(ch,no,order['buyer_id'] if order else None)
        buyer_id=str(order['buyer_id']) if order and order['buyer_id'] else ''
        extra=await sync_ticket_state_after_rename(ch,old_status,status,buyer_id,service)
        if extra: warn+=extra
        if order:
            rr=q('SELECT * FROM orders WHERE id=?',(order['id'],),True)[0]
            log_order(order['id'],i.guild.id,i.user.id,'改名工單',f'{old_service}/{old_status} → {service}/{status}｜{new_name}')
            await status_announce(rr,i.guild)
    except Exception as e:
        import traceback; traceback.print_exc()
        warn=f'\n⚠️ 名稱已更新，但後續同步（分類／權限／狀態公告）發生錯誤：`{str(e)[:150]}`'
    if status=='結單' and '🔒' not in warn and '客人權限**沒有**拔除' not in warn:
        warn+='\n⚠️ 無法確認客人權限是否已拔除，請檢查頻道權限。'
    await reply(f'✅ 已直接改名為：`{new_name}`{warn}')


@bot.tree.command(name='改名工單',description='直接依選擇的服務、狀態、額度與數量改名目前工單')
@app_commands.describe(服務='選擇幣號或代肝',狀態='依服務顯示對應狀態',額度='依服務顯示對應額度',數量='幣號數量；代肝固定為 1')
@app_commands.choices(服務=choices(['幣號','代肝']))
@app_commands.autocomplete(狀態=_rename_status_autocomplete,額度=_rename_amount_autocomplete)
async def rename_ticket_command(i: discord.Interaction,服務:str,狀態:str,額度:str,數量:int=1):
    if not admin(i.user):
        return await i.response.send_message('❌ 你沒有權限使用這個指令。',ephemeral=True)
    # 先 defer，後面的改名／同步可能超過 3 秒。
    await i.response.defer(ephemeral=True)
    try:
        ch=i.channel
        if not isinstance(ch,discord.TextChannel):
            return await i.followup.send('❌ 請在工單頻道內使用 `/改名工單`。',ephemeral=True)
        service='代肝' if str(服務).strip()=='代肝' else '幣號'
        status=str(狀態).strip(); amount=str(額度).strip().upper().replace('Ｍ','M')
        allowed_statuses=RENAME_BOOST_STATUSES if service=='代肝' else RENAME_COIN_STATUSES
        if status not in allowed_statuses:
            return await i.followup.send('❌ 狀態與服務不符合，請重新選擇。',ephemeral=True)
        # 額度不再限定必須是清單內的值，只要是「數字M」即可（autocomplete 只是建議）。
        if not re.fullmatch(r'\d+(?:\.\d+)?M',amount):
            return await i.followup.send('❌ 額度格式錯誤，請輸入例如 `50M`、`100M`。',ephemeral=True)
        if not _resolve_ticket_number(ch):
            return await i.followup.send('❌ 無法自動辨識這張工單的工單號，未執行改名。',ephemeral=True)
        if service=='幣號' and not 1<=int(數量)<=1000:
            return await i.followup.send('❌ 幣號數量請輸入 1～1000。',ephemeral=True)
        qty=1 if service=='代肝' else int(數量)
        return await _complete_manual_rename(i,service,status,amount,qty)
    except Exception as e:
        import traceback; traceback.print_exc()
        try: await i.followup.send(f'❌ /改名工單 發生錯誤：`{str(e)[:180]}`',ephemeral=True)
        except Exception: pass

@bot.tree.command(name='修改狀態',description='修改目前工單狀態')
@app_commands.describe(工單='選擇工單頻道',狀態='新狀態')
@app_commands.choices(狀態=choices(STATUSES))
async def change_status(i,工單:discord.TextChannel,狀態:str):
    if not admin(i.user): return await deny(i)
    await i.response.defer(ephemeral=True)
    try:
        r=q('SELECT * FROM orders WHERE channel_id=? ORDER BY id DESC LIMIT 1',(str(工單.id),),True)
        if not r: return await i.followup.send('❌ 找不到這張工單的訂單紀錄。',ephemeral=True)
        o=r[0]; old_status=str(o['status'])
        q('UPDATE orders SET status=?,completed_at=? WHERE id=?',(狀態,now() if 狀態=='結單' else None,o['id']))
        name=rename_name(狀態,o['ticket_no'],o['product'],o['quantity'],service_type=o['service_type'])
        rename_ok=True
        if 工單.name!=name:
            rename_ok=await _safe_edit_channel(工單,timeout=15,name=name,reason=f'CT小舖 /修改狀態｜操作者 {i.user.id}')
        warn=''
        try:
            extra=await sync_ticket_state_after_rename(工單,old_status,狀態,o['buyer_id'],str(o['service_type'] or '幣號'))
            if extra: warn+=extra
            log_order(o['id'],i.guild.id,i.user.id,'修改狀態',f'{old_status} -> {狀態}'); await post_channel_log(i.guild,'action_log_channel_id',f'👑 **修改狀態**｜#{o["ticket_no"]}｜{old_status} → {狀態}｜{i.user.mention}')
            rr=q('SELECT * FROM orders WHERE id=?',(o['id'],),True)[0]; await status_announce(rr,i.guild)
        except Exception as e:
            traceback.print_exc(); warn=f'\n⚠️ 後續同步（分類／權限／公告）發生錯誤：`{str(e)[:150]}`'
        if not rename_ok: warn+='\n⚠️ 頻道改名失敗或被 Discord 限速（同頻道 10 分鐘內只能改名 2 次），名稱未更新。'
        await i.followup.send(f'✅ {工單.mention} 已更新為 **{狀態}**。{warn}',ephemeral=True)
    except Exception as e:
        traceback.print_exc()
        try: await i.followup.send(f'❌ /修改狀態 發生錯誤：`{str(e)[:180]}`',ephemeral=True)
        except Exception: pass

async def _close_ticket_core(i, command_label='結單'):
    if not admin(i.user): return await deny(i)
    if not i.response.is_done():
        await i.response.defer(ephemeral=True)
    ch=i.channel
    if not isinstance(ch,discord.TextChannel): return await i.followup.send('❌ 請在工單頻道使用。',ephemeral=True)
    r=q('SELECT * FROM orders WHERE channel_id=? ORDER BY id DESC LIMIT 1',(str(ch.id),),True)
    if r:
        o=r[0]; old_status=str(o['status'] or '')
        q('UPDATE orders SET status=?,completed_at=? WHERE id=?',('結單',now(),o['id']))
        rr=q('SELECT * FROM orders WHERE id=?',(o['id'],),True)[0]
        service_type=str(o['service_type'] or '幣號') if 'service_type' in o.keys() else '幣號'
        ticket_number=str(o['ticket_no'])
        buyer_id=o['buyer_id']
        product=str(o['product'] or '50M')
        qty=max(1,int(o['quantity'] or 1))
    else:
        # 沒有訂單的舊／外部工單也能正常關閉，不再卡在「找不到訂單紀錄」。
        ticket_number=_resolve_ticket_number(ch) or '0000'
        service_type=_service_from_name(ch.name) or '幣號'
        status=_status_from_name(ch.name) or '待處理'
        product,qty=_amount_qty_from_name(ch.name,service_type)
        product=product or '50M'; qty=max(1,int(qty or 1))
        buyer_id=(await buyer_for(ch))[0]
        old_status=status
        rr=None

    final_name=rename_name('結單',f'{int(ticket_number):04d}' if str(ticket_number).isdigit() else ticket_number,product,qty,service_type=service_type)
    rename_ok=True
    if ch.name!=final_name:
        rename_ok=await _safe_edit_channel(ch,timeout=10,name=final_name,reason=f'{command_label}自動改名')

    moved=False
    for attempt in range(2):
        moved=await move_closed_ticket_to_service_category(ch,service_type)
        if moved: break
        await asyncio.sleep(0.25)
    ok,perm_msg=await revoke_buyer_access(ch,buyer_id)
    if r:
        log_order(r[0]['id'],i.guild.id,i.user.id,command_label,f'{old_status} → 結單｜{perm_msg}')
    await post_channel_log(i.guild,'action_log_channel_id',f'👑 **{command_label}**｜#{ticket_number}｜{old_status} → 結單｜{i.user.mention}｜{perm_msg}')
    try:
        await ch.send(f'🔒 **工單已關閉**\n狀態：結單\n🔒 {perm_msg}\n📁 結單分類：{"已移入" if moved else "移動失敗，請確認已設定結單分類且機器人有管理頻道權限"}',view=ClosedTicketView())
    except discord.HTTPException as e:
        print(f'[CLOSE] send close message failed｜channel={ch.id}｜error={e!r}')
    await i.followup.send(f'✅ 工單 #{ticket_number} 已結單。\n🔒 {perm_msg}\n📁 {"已移入對應結單分類" if moved else "結單分類移動失敗：請先用 /建立服務工單分類 或 /設定服務工單分類 設定，並確認機器人有管理頻道權限"}'+('' if rename_ok else '\n⚠️ 頻道改名失敗或被 Discord 限速（同頻道 10 分鐘內只能改名 2 次），名稱未更新。'),ephemeral=True)
    if rr is not None:
        rr=q('SELECT * FROM orders WHERE id=?',(rr['id'],),True)[0]
        await status_announce(rr,i.guild)

@bot.tree.command(name='結單',description='結束工單、拔除客人存取權並移入結單分類')
async def close_ticket(i):
    await _close_ticket_core(i,'結單')

@bot.tree.command(name='關閉工單',description='關閉目前工單、移除客人存取權並移入結單分類')
async def close_ticket_command(i):
    await _close_ticket_core(i,'關閉工單')

@bot.tree.command(name='查詢工單',description='查詢工單')
@app_commands.describe(狀態='可不填',商品='可不填')
@app_commands.choices(狀態=choices(STATUSES),商品=choices(QUERY_PRODUCT_CHOICES))
async def query_orders(i,狀態:str|None=None,商品:str|None=None):
    if not admin(i.user): return await deny(i)
    await i.response.defer(ephemeral=True)
    sql='SELECT * FROM orders WHERE guild_id=?'; params=[str(i.guild.id)]
    if 狀態: sql+=' AND status=?'; params.append(狀態)
    if 商品: sql+=' AND product=?'; params.append(商品)
    sql+=' ORDER BY id DESC LIMIT 50'; rows=q(sql,params,True)
    if not rows: return await i.followup.send('📋 目前沒有符合條件的工單。',ephemeral=True)
    lines=[]
    for o in rows:
        try: ch=i.guild.get_channel(int(o['channel_id']))
        except (TypeError,ValueError): ch=None
        link=ch.jump_url if isinstance(ch,discord.TextChannel) else ''
        lines.append(f'`#{o["ticket_no"]}` {o["product"]} × {o["quantity"]}｜**{o["status"]}**'+(f'｜[前往工單]({link})' if link else ''))
    # Discord 單則訊息上限 2000 字，50 筆會超過，所以分段送出。
    chunks=[]; cur='📋 **工單查詢**'
    for ln in lines:
        if len(cur)+1+len(ln)>1900: chunks.append(cur); cur=ln
        else: cur+='\n'+ln
    chunks.append(cur)
    for c in chunks: await i.followup.send(c,ephemeral=True)

@bot.tree.command(name='今日統計',description='查看今天的訂單統計與金額')
async def today_stats(i):
    if not admin(i.user): return await deny(i)
    await i.response.defer(ephemeral=True)
    from zoneinfo import ZoneInfo
    tz=ZoneInfo('Asia/Taipei'); today=datetime.now(tz).date()
    rows=q('SELECT * FROM orders WHERE guild_id=?',(str(i.guild.id),),True)
    todays=[]
    for o in rows:
        try:
            dt=datetime.fromisoformat(o['created_at']).astimezone(tz)
            if dt.date()==today: todays.append(o)
        except Exception: pass
    total=sum(int(o['total_price']) for o in todays)
    paid=sum(int(o['total_price']) for o in todays if o['status'] not in ('待付款','已取消'))
    counts={st:sum(1 for o in todays if o['status']==st) for st in STATUSES}
    lines=[f'📊 **今日訂單統計｜{today.strftime("%Y/%m/%d")}**',f'🧾 訂單數：**{len(todays)} 筆**',f'💰 訂單總額：**NT${total:,}**',f'💳 已進入付款後流程：**NT${paid:,}**']
    lines.append('')
    lines += [f'🟡 待付款：{counts["待付款"]} 筆',f'🔵 待交貨：{counts["待交貨"]} 筆',f'🛠️ 處理中：{counts["處理中"]} 筆',f'📦 待收貨：{counts["待收貨"]} 筆',f'✅ 結單：{counts["結單"]} 筆',f'❌ 已取消：{counts["已取消"]} 筆']
    await i.followup.send('\n'.join(lines),ephemeral=True)

@bot.tree.command(name='查詢餘額',description='查詢自己的或指定會員的餘額')
@app_commands.describe(會員='管理員可指定其他會員；一般會員留空')
async def balance(i,會員:discord.Member|None=None):
    await i.response.defer(ephemeral=True)
    target=會員 if admin(i.user) and 會員 else i.user
    r=q('SELECT balance FROM balances WHERE user_id=?',(str(target.id),),True); b=int(r[0]['balance']) if r else 0
    await i.followup.send(f'💰 {target.mention} 目前餘額：**NT${b:,}**',ephemeral=True)

async def balance_change(i,會員,金額,action):
    if not admin(i.user): return await deny(i)
    await i.response.defer(ephemeral=True)
    if 金額<=0: return await i.followup.send('❌ 金額必須大於 0。',ephemeral=True)
    # 餘額是金錢資料，必須在同一個 transaction 內讀取＋更新＋寫入流水，避免兩個管理員同時操作造成覆蓋。
    async with lock:
        with db:
            cur=db.cursor()
            cur.execute('SELECT balance FROM balances WHERE user_id=?',(str(會員.id),))
            row=cur.fetchone(); old=int(row['balance']) if row else 0
            new=old+金額 if action=='增加' else old-金額
            if new<0:
                return await i.followup.send(f'❌ {會員.mention} 餘額不足，無法扣除 NT${金額:,}。',ephemeral=True)
            cur.execute('INSERT INTO balances(user_id,balance) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET balance=excluded.balance',(str(會員.id),new))
            cur.execute('INSERT INTO balance_logs(guild_id,user_id,operator_id,amount,balance_after,action,created_at,note) VALUES(?,?,?,?,?,?,?,?)',(str(i.guild.id),str(會員.id),str(i.user.id),金額,new,action,now(),''))
    await post_channel_log(i.guild,'action_log_channel_id',f'💰 **餘額{action}**｜{會員.mention}｜NT${金額:,}｜操作人：{i.user.mention}｜餘額：NT${new:,}')
    await i.followup.send(f'✅ {會員.mention} 餘額已從 NT${old:,} 變為 **NT${new:,}**。',ephemeral=True)

@bot.tree.command(name='增加餘額',description='增加會員餘額')
async def add_balance(i,會員:discord.Member,金額:int): await balance_change(i,會員,金額,'增加')
@bot.tree.command(name='扣除餘額',description='扣除會員餘額')
async def sub_balance(i,會員:discord.Member,金額:int): await balance_change(i,會員,金額,'扣除')

@bot.tree.command(name='購買相關資訊',description='查看購買相關資訊')
async def buy_info(i):
    text=get(i.guild.id,'buy_info','請先確認商品、數量、價格及交易規則後再付款。\n如有疑問，請於付款前提出。')
    await i.response.send_message(text,ephemeral=True)

@bot.tree.command(name='設定購買資訊',description='設定購買相關資訊')
async def set_buy_info(i,內容:str):
    if not admin(i.user): return await deny(i)
    setv(i.guild.id,'buy_info',內容); await i.response.send_message('✅ 購買相關資訊已更新。',ephemeral=True)

@bot.tree.command(name='設定管理員',description='設定管理員身分組')
async def set_admin_role(i,身分組:discord.Role):
    if not i.user.guild_permissions.administrator: return await deny(i)
    setv(i.guild.id,'admin_role_id',身分組.id); await i.response.send_message(f'✅ 已設定管理員身分組為 {身分組.mention}。',ephemeral=True)

@bot.tree.command(name='設定店長公告',description='設定店長休息時顯示的公告')
async def set_rest_text(i,內容:str):
    if not admin(i.user): return await deny(i)
    setv(i.guild.id,'rest_text',內容); await i.response.send_message('✅ 店長休息公告已更新。',ephemeral=True)



@bot.tree.command(name='開啟無限制開單',description='店長開啟後，同一位客人可以同時建立多張未結單工單')
async def enable_unlimited_ticket_opening(i):
    if not admin(i.user): return await deny(i)
    setv(i.guild.id,'unlimited_ticket_opening','1')
    await i.response.send_message('✅ 已開啟「無限制開單」。客人可以同時建立多張未結單工單。',ephemeral=True)

@bot.tree.command(name='關閉無限制開單',description='關閉同一位客人同時建立多張未結單工單的功能')
async def disable_unlimited_ticket_opening(i):
    if not admin(i.user): return await deny(i)
    setv(i.guild.id,'unlimited_ticket_opening','0')
    await i.response.send_message('✅ 已關閉「無限制開單」。恢復每位客人只能保留一張未結單工單。',ephemeral=True)

@bot.tree.command(name='無限制開單狀態',description='查看目前是否開啟無限制開單')
async def unlimited_ticket_opening_status(i):
    if not admin(i.user): return await deny(i)
    enabled=get(i.guild.id,'unlimited_ticket_opening','0') == '1'
    await i.response.send_message(f"📋 無限制開單目前為：{'🟢 開啟' if enabled else '🔴 關閉'}",ephemeral=True)

@bot.tree.command(name='設定開單身分組',description='設定可以查看所有工單的指定身分組')
async def set_ticket_role(i,身分組:discord.Role):
    if not admin(i.user): return await deny(i)
    if 身分組 == i.guild.default_role: return await i.response.send_message('❌ 不能把 @everyone 設為工單身分組，否則所有會員都可能看見工單。',ephemeral=True)
    setv(i.guild.id,'ticket_staff_role_id',身分組.id); await i.response.send_message(f'✅ 工單指定身分組已設定為 {身分組.mention}。',ephemeral=True)

@bot.tree.command(name='設定工單訊息',description='設定新工單建立後的第一則訊息')
async def set_ticket_welcome(i,內容:str):
    if not admin(i.user): return await deny(i)
    if len(內容) > 2000:
        return await i.response.send_message('❌ 工單第一則訊息最多 2000 字，請縮短後再設定。',ephemeral=True)
    setv(i.guild.id,'ticket_welcome',內容)
    await i.response.send_message('✅ 新工單第一則訊息已更新。',ephemeral=True)

@bot.tree.command(name='設定開單分類',description='設定新工單建立在哪個分類')
async def set_ticket_category(i,分類:discord.CategoryChannel):
    if not admin(i.user): return await deny(i)
    setv(i.guild.id,'ticket_category_id',分類.id); await i.response.send_message(f'✅ 開單分類已設定為 {分類.name}。',ephemeral=True)

class ManualRouteView(discord.ui.View):
    """V69 手動分流：管理員自己選工單與目標分類；不改工單名稱、不改訂單狀態。"""
    def __init__(self, guild, max_tickets=1):
        super().__init__(timeout=300)
        self.guild=guild
        self.guild_id=guild.id
        self.max_tickets=max(1,min(int(max_tickets),25))
        self.target_category_id=None
        self.ticket_ids=[]

        self.target_select=discord.ui.ChannelSelect(
            placeholder='① 選擇要移入的目標分類',
            channel_types=[discord.ChannelType.category],
            min_values=1,max_values=1,row=0
        )
        self.target_select.callback=self.on_target
        self.add_item(self.target_select)

        self.ticket_select=discord.ui.ChannelSelect(
            placeholder='② 選擇要移動的工單（可複選）',
            channel_types=[discord.ChannelType.text],
            min_values=1,max_values=self.max_tickets,row=1
        )
        self.ticket_select.callback=self.on_tickets
        self.add_item(self.ticket_select)

        self.move_button=discord.ui.Button(label='🚚 執行手動分流',style=discord.ButtonStyle.success,row=2)
        self.move_button.callback=self.execute_move
        self.add_item(self.move_button)

    async def interaction_check(self,i):
        if not admin(i.user):
            await i.response.send_message('❌ 只有管理員可以操作手動分流。',ephemeral=True)
            return False
        return True

    async def on_target(self,i):
        vals=self.target_select.values or []
        cat=vals[0] if vals else None
        self.target_category_id=getattr(cat,'id',None)
        await i.response.send_message(f'📁 目標分類已選擇：**{getattr(cat,"name","未知分類")}**。',ephemeral=True)

    async def on_tickets(self,i):
        self.ticket_ids=[getattr(ch,'id',None) for ch in (self.ticket_select.values or []) if getattr(ch,'id',None)]
        names=[f'`{getattr(ch,"name","未知工單")}`' for ch in (self.ticket_select.values or [])]
        await i.response.send_message('🎫 已選工單：'+('、'.join(names) if names else '無'),ephemeral=True)

    async def execute_move(self,i):
        await i.response.defer(ephemeral=True)
        if not self.target_category_id:
            return await i.followup.send('❌ 請先選擇目標分類。',ephemeral=True)
        if not self.ticket_ids:
            return await i.followup.send('❌ 請先選擇至少一張工單。',ephemeral=True)
        category=self.guild.get_channel(int(self.target_category_id))
        if not isinstance(category,discord.CategoryChannel):
            return await i.followup.send('❌ 目標分類不存在，請重新開啟手動分流。',ephemeral=True)
        if len(category.channels)+len(self.ticket_ids)>50:
            # 實際會扣掉原本就在該分類的頻道，避免誤判。
            current=sum(1 for cid in self.ticket_ids if self.guild.get_channel(int(cid)) is not None and getattr(self.guild.get_channel(int(cid)),'category_id',None)==category.id)
            if len(category.channels)-current+len(self.ticket_ids)>50:
                return await i.followup.send(f'❌ 目標分類最多只能有 50 個頻道，目前無法一次移入這批工單。',ephemeral=True)
        moved=[]; failed=[]; skipped=[]
        for cid in self.ticket_ids:
            ch=self.guild.get_channel(int(cid))
            if not isinstance(ch,discord.TextChannel):
                failed.append(str(cid)); continue
            no=_channel_ticket_number(ch)
            if no is None:
                skipped.append(ch.name); continue
            if ch.category_id==category.id:
                skipped.append(ch.name); continue
            try:
                await ch.edit(category=category,reason=f'CT小舖 V69 手動分流｜操作者 {i.user.id}')
                moved.append(ch.name)
            except (discord.Forbidden,discord.HTTPException) as e:
                print(f'[MANUAL_ROUTE] 移動失敗｜channel={ch.id}｜target={category.id}｜error={e!r}')
                failed.append(f'{ch.name}：{e}')
            await asyncio.sleep(0.08)
        lines=[f'🚚 **手動分流完成**｜目標：{category.mention}',f'成功：**{len(moved)}**',f'跳過：**{len(skipped)}**',f'失敗：**{len(failed)}**']
        if moved: lines.append('\n已移動：'+ '、'.join(f'`{x}`' for x in moved[:20]))
        if skipped: lines.append('\n已跳過：'+ '、'.join(f'`{x}`' for x in skipped[:20]))
        if failed: lines.append('\n失敗：'+ '、'.join(f'`{x}`' for x in failed[:10]))
        await i.followup.send('\n'.join(lines),ephemeral=True)
        self.stop()

@bot.tree.command(name='手動操作分流',description='手動選擇工單並移動到指定分類，不改工單名稱')
async def manual_route_command(i):
    await i.response.defer(ephemeral=True)
    if not admin(i.user): return await i.followup.send('❌ 你沒有權限使用這個指令。',ephemeral=True)
    await i.followup.send('🗂️ **手動操作分流**\n請先選擇「目標分類」，再選擇要移動的工單，最後按「執行手動分流」。\n\n📌 此操作只移動頻道，不會自動改名、不會改訂單狀態。',view=ManualRouteView(i.guild,1),ephemeral=True)

@bot.tree.command(name='批量移動工單',description='手動選擇多張工單並一次移動到指定分類')
async def batch_move_tickets_command(i):
    await i.response.defer(ephemeral=True)
    if not admin(i.user): return await i.followup.send('❌ 你沒有權限使用這個指令。',ephemeral=True)
    await i.followup.send('📦 **批量移動工單**\n一次最多手動選 25 張工單。先選目標分類，再選工單，最後按「執行手動分流」。\n\n📌 不會改工單名稱、不會改訂單狀態。',view=ManualRouteView(i.guild,25),ephemeral=True)

@bot.tree.command(name='設定服務工單分類',description='設定結單後的幣號分類與代肝分類')
async def set_service_ticket_categories(i, 幣號分類:discord.CategoryChannel, 代肝分類:discord.CategoryChannel):
    if not admin(i.user): return await deny(i)
    if 幣號分類.id == 代肝分類.id:
        return await i.response.send_message('❌ 幣號與代肝不能使用同一個分類。',ephemeral=True)
    setv(i.guild.id,'service_category_coin_id',str(幣號分類.id))
    setv(i.guild.id,'service_category_boost_id',str(代肝分類.id))
    await i.response.send_message(f'✅ 已設定固定服務分類。\n🪙 幣號 → {幣號分類.mention}\n🛠️ 代肝 → {代肝分類.mention}\n\n之後只有工單「結單」時才會自動移入對應分類；開單、選服務、付款流程都不會自動移入結單分類。',ephemeral=True)

@bot.tree.command(name='建立服務工單分類',description='自動建立「幣號結單」與「代肝結單」兩個固定分類')
async def create_service_ticket_categories(i):
    await i.response.defer(ephemeral=True)
    if not admin(i.user): return await i.followup.send('❌ 你沒有權限使用這個指令。',ephemeral=True)
    me=i.guild.me
    if me is None or not me.guild_permissions.manage_channels:
        return await i.followup.send('❌ 機器人需要「管理頻道」權限才能建立分類。',ephemeral=True)
    result=[]
    template_id=get(i.guild.id,'ticket_category_id','')
    template=i.guild.get_channel(int(template_id)) if template_id.isdigit() else None
    overwrites=template.overwrites if isinstance(template,discord.CategoryChannel) else None
    for service,key,label in [('幣號','service_category_coin_id','幣號結單'),('代肝','service_category_boost_id','代肝結單')]:
        cat=await ensure_service_category(i.guild,service)
        if not isinstance(cat,discord.CategoryChannel):
            existing=discord.utils.get(i.guild.categories,name=label) or discord.utils.get(i.guild.categories,name=label.replace('結單','工單'))
            if existing is not None:
                cat=existing
            else:
                try:
                    kwargs={'name':label,'reason':'CT小舖建立固定服務工單分類'}
                    if overwrites is not None: kwargs['overwrites']=overwrites
                    cat=await i.guild.create_category(**kwargs)
                except discord.Forbidden:
                    return await i.followup.send('❌ 建立分類失敗：機器人沒有管理頻道權限。',ephemeral=True)
                except discord.HTTPException as e:
                    return await i.followup.send(f'❌ 建立分類失敗：{e}',ephemeral=True)
            setv(i.guild.id,key,str(cat.id))
        result.append(f'{"🪙" if service=="幣號" else "🛠️"} {cat.mention}')
    await i.followup.send('✅ 固定服務分類已建立／設定。\n'+'\n'.join(result),ephemeral=True)

@bot.tree.command(name='查看服務工單分類',description='查看目前結單後的幣號／代肝分類')
async def show_service_ticket_categories(i):
    if not admin(i.user): return await deny(i)
    await i.response.defer(ephemeral=True)
    coin=await ensure_service_category(i.guild,'幣號'); boost=await ensure_service_category(i.guild,'代肝')
    await i.followup.send(f'🪙 幣號結單分類：{coin.mention if coin else "❌ 尚未設定"}\n🛠️ 代肝結單分類：{boost.mention if boost else "❌ 尚未設定"}\n\n📌 開啟中的工單不移動；只有結單後才自動歸類。',ephemeral=True)

@bot.tree.command(name='設定自動分類',description='設定頻道自動移動到指定分類並排序')
async def setup_auto_category(i):
    # V34：Slash command 的第一件事就是 ACK，連權限判斷都放到 ACK 後，避免 DB/Member 快取造成 3 秒逾時。
    try:
        await i.response.defer(ephemeral=True)
        if not admin(i.user):
            return await i.followup.send('❌ 你沒有權限使用這個指令。',ephemeral=True)
        view=AutoCategorySetupView(i.guild, operator_id=i.user.id)
        view.guild=i.guild
        cfg=_auto_category_config(i.guild.id)
        status=f'\n\n目前總開關：{"🟢 開啟" if cfg["enabled"] else "🔴 關閉"}'
        await i.edit_original_response(content=view._render(i.guild)+status,view=view)
    except discord.NotFound as e:
        print(f'[AUTO_CATEGORY_SETUP] Discord API 互動已過期／不存在｜guild={getattr(i.guild,"id",None)}｜error={e!r}')
    except Exception as e:
        print(f'[AUTO_CATEGORY_SETUP] 錯誤｜guild={getattr(i.guild,"id",None)}｜error={e!r}')
        traceback.print_exc()
        try: await i.followup.send(f'❌ 自動分類設定面板開啟失敗：`{str(e)[:180]}`',ephemeral=True)
        except Exception: pass

@bot.tree.command(name='關閉自動分類',description='關閉頻道自動分類')
async def disable_auto_category(i):
    if not admin(i.user): return await deny(i)
    setv(i.guild.id,'auto_category_enabled','0')
    await i.response.send_message('🔴 自動分類已關閉。已存在的頻道不會被移動。',ephemeral=True)

@bot.tree.command(name='開啟自動分類',description='重新開啟已設定的頻道自動分類')
async def enable_auto_category(i):
    if not admin(i.user): return await deny(i)
    cfg=_auto_category_config(i.guild.id)
    if not cfg['source_ids'] or not cfg['destination_id']:
        return await i.response.send_message('❌ 尚未設定自動分類。請先使用 `/設定自動分類`。',ephemeral=True)
    setv(i.guild.id,'auto_category_enabled','1')
    await i.response.send_message(f'🟢 自動分類已開啟。目標：<#{cfg["destination_id"]}>｜排序：{cfg["sort_mode"]}',ephemeral=True)

@bot.tree.command(name='立即整理分類',description='依目前自動分類設定立即整理來源分類內的頻道')
async def run_auto_category_now(i):
    await i.response.defer(ephemeral=True)
    if not admin(i.user): return await i.followup.send('❌ 你沒有權限使用這個指令。',ephemeral=True)
    cfg=_auto_category_config(i.guild.id)
    if not cfg['source_ids'] or not cfg['destination_id']:
        return await i.followup.send('❌ 尚未設定自動分類。請先使用 `/設定自動分類`。',ephemeral=True)
    moved,failed,err=await auto_categorize_channels(i.guild,cfg['source_ids'],cfg['destination_id'],cfg['sort_mode'])
    if err: return await i.followup.send(f'❌ 整理失敗：{err}',ephemeral=True)
    await i.followup.send(f'✅ 整理完成。\n已移動：{moved} 個頻道\n失敗：{failed} 個\n排序：{cfg["sort_mode"]}',ephemeral=True)


# 自動偵測工單
async def delayed(ch):
    """偵測外部／舊 Ticket Bot 建立的工單。每個頻道只允許一條偵測流程。"""
    if not isinstance(ch, discord.TextChannel): return
    key=str(ch.id)
    lk=DETECTION_LOCKS.setdefault(key, asyncio.Lock())
    if lk.locked(): return
    async with lk:
        try:
            await asyncio.sleep(1.0)
            current=ch.guild.get_channel(ch.id)
            if not isinstance(current, discord.TextChannel): return
            if ticket_record(current): return
            if not is_ticket_candidate(current): return

            no=ticket_no(current.name)
            if not no: return
            remember(current,no)

            # 舊／外部工單也不再自動發送交易／服務選擇面板。
            await asyncio.sleep(1.0)
            current=current.guild.get_channel(current.id)
            if not isinstance(current, discord.TextChannel): return
            if ticket_no(current.name) != no: return
            if is_ticket_candidate(current):
                try:
                    await current.edit(name=f'工單-{int(no):04d}', reason='CT小舖自動辨識工單號')
                except discord.HTTPException as e:
                    print(f'[TICKET_DETECT] 自動改名失敗｜channel={current.id}｜error={e!r}')
            # V68：自動偵測只統一名稱，不把開啟中的工單移到結單分類。
        finally:
            if DETECTION_LOCKS.get(key) is lk: DETECTION_LOCKS.pop(key,None)

@bot.event
async def on_message(message):
    if message.author.bot or not isinstance(message.channel,discord.TextChannel):
        return

    # 自動算價：只偵測設定的店長本人訊息，不是 Slash Command。
    # 例如：50*3、100M*5、50*3+100*2。
    # 可用 /關閉自動算價 關閉；預設開啟。
    if get(message.guild.id, 'auto_price_calc', '1') == '1':
        owner_id = get(message.guild.id, 'ticket_owner_id', '')
        is_calc_operator = admin(message.author) or (owner_id and str(message.author.id) == str(owner_id))
        if is_calc_operator:
            calc = parse_price_calculation(message.guild, message.content)
            if calc is not None:
                try:
                    await message.channel.send(calc['text'], allowed_mentions=discord.AllowedMentions.none())
                except discord.HTTPException as e:
                    print(f'[PRICE_CALC] 發送試算結果失敗｜guild={message.guild.id}｜channel={message.channel.id}｜error={e!r}')
                return

    # 付款圖片偵測不依賴 TICKET_CHANNEL_IDS 快取，避免工單剛建立／重啟 Bot 後漏偵測。
    # 客人可以先按「完成付款」再上傳，也可以先上傳圖片再按「完成付款」。
    proof=q("SELECT * FROM orders WHERE channel_id=? AND buyer_id=? AND status='待確認付款' ORDER BY id DESC LIMIT 1",(str(message.channel.id),str(message.author.id)),True)
    if proof and message.attachments:
        image_exts=('.png','.jpg','.jpeg','.webp','.gif','.bmp','.heic','.heif')
        image=next((a for a in message.attachments if (a.content_type or '').lower().startswith('image/') or (getattr(a,'filename','') or '').lower().endswith(image_exts)),None)
        if image:
            o=proof[0]
            q('UPDATE orders SET payment_proof_url=?,payment_proof_message_id=? WHERE id=?',(image.url,str(message.id),o['id']))
            log_order(o['id'],message.guild.id,message.author.id,'收到付款明細',image.url)
            rr=q('SELECT * FROM orders WHERE id=?',(o['id'],),True)[0]
            await message.channel.send(f'🧾 **已收到付款明細**\\n付款明細已綁定至訂單 #{o["ticket_no"]}。\\n⏳ 目前等待店長確認付款。\\n\\n👑 店長現在可以按「💰 確認付款」。', view=OrderManageView(o['id']))
            await post_channel_log(message.guild,'order_log_channel_id',f'🧾 **收到付款明細**｜#{o["ticket_no"]}｜{message.author.mention}｜{image.url}')
            await status_announce(rr,message.guild)
            return

    # 普通頻道完全不碰 SQLite；只有已知工單才進入後續處理。
    if str(message.channel.id) not in TICKET_CHANNEL_IDS:
        await bot.process_commands(message)
        return
    # 代肝付款確認後，客人在工單直接傳遊戲帳號即可。
    rows=q("SELECT * FROM orders WHERE channel_id=? AND buyer_id=? AND service_type='代肝' AND status='待排單' AND game_account IS NULL ORDER BY id DESC LIMIT 1",(str(message.channel.id),str(message.author.id)),True)
    if rows and message.content.strip():
        o=rows[0]
        q('UPDATE orders SET game_account=? WHERE id=?',(message.content.strip(),o['id']))
        log_order(o['id'],message.guild.id,message.author.id,'收到遊戲帳號','客人於工單提供遊戲帳號')
        await message.channel.send('🎮 **已收到遊戲帳號**\n您的代肝訂單已排入處理。\n📋 排隊進度請至排隊網站查看。')
        await post_channel_log(message.guild,'order_log_channel_id',f'🎮 **已收到遊戲帳號**｜#{o["ticket_no"]}｜{message.author.mention}')
        rr=q('SELECT * FROM orders WHERE id=?',(o['id'],),True)[0]
        await status_announce(rr,message.guild)
        return
    await bot.process_commands(message)

@bot.event
async def on_guild_channel_create(ch):
    if isinstance(ch,discord.TextChannel):
        asyncio.create_task(delayed(ch))
        asyncio.create_task(_auto_category_on_create(ch))

@bot.event
async def on_guild_channel_delete(ch):
    TICKET_CHANNEL_IDS.discard(str(ch.id))

@bot.event
async def on_guild_join(guild):
    try:
        bot.tree.copy_global_to(guild=guild)
        synced=await bot.tree.sync(guild=guild)
        print(f'新伺服器同步成功：{guild.name} ({guild.id})，{len(synced)} 個指令')
    except Exception as e:
        print('guild join sync error',guild.id,repr(e))

a=asyncio.Lock()
@tasks.loop(seconds=60)
async def payment_reminder_loop():
    from datetime import timedelta
    for g in bot.guilds:
        try: mins=int(get(g.id,'payment_reminder_minutes','15') or 15)
        except ValueError: mins=15
        if mins<=0: continue
        cutoff=datetime.now(timezone.utc)-timedelta(minutes=mins)
        rows=q("SELECT * FROM orders WHERE guild_id=? AND status='待付款' AND payment_reminder_sent=0",(str(g.id),),True)
        for o in rows:
            try: created=datetime.fromisoformat(o['created_at'])
            except Exception: continue
            if created>cutoff: continue
            ch=g.get_channel(int(o['channel_id']))
            q('UPDATE orders SET payment_reminder_sent=1 WHERE id=?',(o['id'],))
            log_order(o['id'],g.id,None,'付款逾時提醒',f'{mins} 分鐘未付款')
            await post_channel_log(g,'order_log_channel_id',f'⏰ **付款提醒**｜#{o["ticket_no"]}｜{o["product"]} × {o["quantity"]}｜NT${o["total_price"]:,}｜已超過 {mins} 分鐘未完成付款。')
            if isinstance(ch,discord.TextChannel):
                try: await ch.send(f'⏰ **付款提醒**\n您的訂單 #{o["ticket_no"]} 尚未完成付款，若仍要購買，請回到付款訊息按下「💳 完成付款」。')
                except discord.HTTPException: pass

@payment_reminder_loop.before_loop
async def before_payment_reminder(): await bot.wait_until_ready()


@bot.event
async def on_error(event_method, *args, **kwargs):
    import traceback
    print(f'Bot event error: {event_method}')
    traceback.print_exc()

def _status_from_name(name):
    n=name or ''
    for st in TICKET_STATUSES:
        if re.match(rf'^{re.escape(st)}(?:[-_ ]|$)', n):
            return st
    # 待倒只屬於代肝的改名狀態；只有完整的「待倒-代肝-工單號」格式才辨識。
    if re.match(r'^待倒[-_ ]+代肝[-_ ]+\d{1,8}(?:[-_ ].*)?$', n, re.I):
        return '待倒'
    return None

def _service_from_name(name):
    n=name or ''
    if '代肝' in n or re.search(r'(?i)(?:^|[-_ ])boost(?:[-_ ]|$)',n): return '代肝'
    if '幣號' in n or '幣号' in n or re.search(r'(?i)(?:^|[-_ ])coin(?:[-_ ]|$)',n): return '幣號'
    # 舊版幣號名稱沒有服務字樣時，只要有『額度＋數量』格式就視為幣號。
    if re.search(r'\d+(?:\.\d+)?\s*[mM]?(?:\s*[xX×*]\s*\d+|\s*[一二兩三四五六七八九十百]+\s*隻|\s*\d+\s*隻)',n): return '幣號'
    return None

def _parse_legacy_qty(token):
    t=str(token or '').strip().replace('兩','二')
    if t.isdigit():
        return int(t)
    nums={'一':1,'二':2,'三':3,'四':4,'五':5,'六':6,'七':7,'八':8,'九':9,'十':10,'百':100}
    if t in nums: return nums[t]
    if len(t)==2 and t[0]=='十' and t[1] in nums: return 10+nums[t[1]]
    if len(t)==2 and t[1]=='十' and t[0] in nums: return nums[t[0]]*10
    if len(t)==3 and t[1]=='十' and t[0] in nums and t[2] in nums: return nums[t[0]]*10+nums[t[2]]
    return None

def _amount_qty_from_name(name, service):
    n=name or ''
    amount=None; qty=None
    patterns=[
        r'(\d+(?:\.\d+)?)\s*[mM]\s*[xX×*]\s*(\d+)',
        r'(\d+(?:\.\d+)?)\s*[xX×*]\s*(\d+)',
        r'(\d+(?:\.\d+)?)\s*[mM]\s*([一二兩三四五六七八九十百]+)\s*隻',
        r'(\d+(?:\.\d+)?)\s*[mM]\s*(\d+)\s*隻',
        r'(\d+(?:\.\d+)?)\s*[mM]\s*(\d+)隻',
    ]
    for pat in patterns:
        ms=list(re.finditer(pat,n,re.I))
        if ms:
            m=ms[-1]; amount=f'{m.group(1)}M'; qty=_parse_legacy_qty(m.group(2)); break
    if amount is None:
        amounts=re.findall(r'(\d+(?:\.\d+)?)\s*[mM]',n)
        if amounts: amount=f'{amounts[-1]}M'
    if service=='幣號' and qty is None:
        ms=list(re.finditer(r'(\d+(?:\.\d+)?)\s*[mM]?\s*([一二兩三四五六七八九十百]+|\d+)\s*隻',n,re.I))
        if ms:
            m=ms[-1]; amount=amount or f'{m.group(1)}M'; qty=_parse_legacy_qty(m.group(2))
    if service=='幣號' and qty is None:
        # 名稱只有額度時，舊工單視為 1 隻；不把額度尾數當成數量。
        qty=1
    elif service!='幣號':
        qty=1
    return amount,qty


@bot.tree.command(name='自動排序工單號', description='依原工單號由小到大重新連號為0001、0002、0003…')
async def auto_sort_ticket_numbers(i: discord.Interaction):
    try:
        await i.response.defer(ephemeral=True)
    except discord.NotFound:
        return
    if not admin(i.user):
        return await i.followup.send('❌ 你沒有權限使用這個指令。',ephemeral=True)
    result=await renumber_all_tickets(i.guild)
    if not result['found']:
        return await i.followup.send('📋 目前沒有辨識到任何工單。',ephemeral=True)
    lines=[f"原 {a} → 新 {b}" for a,b,_ in result['mapping'][:30]]
    more='' if len(result['mapping'])<=30 else f"\n……另外還有 {len(result['mapping'])-30} 單已整理。"
    msg=(f"✅ 工單號自動排序完成\n\n"
         f"共偵測：**{result['found']}** 單\n"
         f"已重新編號：**{result['changed']}** 單\n"
         f"失敗：**{result['failed']}** 單\n\n"+"\n".join(lines)+more)
    await i.followup.send(msg,ephemeral=True)


@bot.tree.command(name='匯出設定', description='將目前伺服器的設定匯出成文字檔')
async def export_settings(i):
    if not admin(i.user): return await deny(i)
    await i.response.defer(ephemeral=True)
    data=config_bytes(i.guild.id)
    stamp=datetime.now().strftime('%Y%m%d_%H%M%S')
    f=discord.File(io.BytesIO(data), filename=f'CT小舖_設定_{stamp}.json')
    await i.followup.send('✅ 已匯出目前設定。\n⚠️ 設定檔可能包含付款資訊、頻道 ID、身分組 ID 等資料，請勿公開分享。\n\n這份檔案可直接交給下一版 CT小舖 Bot 使用「/匯入設定」。',file=f,ephemeral=True)


@bot.tree.command(name='匯入設定', description='從 CT小舖 設定文字檔恢復設定')
@app_commands.describe(檔案='使用 /匯出設定 產生的 .json 設定檔（舊版 .txt JSON 也可）')
async def import_settings(i, 檔案:discord.Attachment):
    if not admin(i.user): return await deny(i)
    await i.response.defer(ephemeral=True)
    if 檔案.size>2*1024*1024: return await i.followup.send('❌ 設定檔過大（上限 2 MB），請使用「/匯出設定」產生的檔案。',ephemeral=True)
    try:
        raw=await 檔案.read(); data=json.loads(raw.decode('utf-8-sig'))
    except (UnicodeDecodeError,json.JSONDecodeError,discord.HTTPException) as e:
        return await i.followup.send(f'❌ 無法讀取設定檔：{e}',ephemeral=True)
    if not isinstance(data,dict) or data.get('format')!='CT小舖設定檔': return await i.followup.send('❌ 這不是 CT小舖 的設定檔。請使用「/匯出設定」產生的檔案。',ephemeral=True)
    try: version=int(data.get('version',0))
    except (TypeError,ValueError): version=0
    if version not in (1,CONFIG_EXPORT_VERSION): return await i.followup.send('❌ 設定檔版本不相容，目前支援 V1 舊檔與 V2 新格式。請重新匯出設定。',ephemeral=True)
    settings=data.get('settings',{}); products=data.get('products',[]); boost_prices_data=data.get('boost_prices',[]); balances_data=data.get('balances',[])
    if version==1: products=[dict(x) for x in products if isinstance(x,dict)]
    if not isinstance(settings,dict) or not isinstance(products,list) or not isinstance(boost_prices_data,list) or not isinstance(balances_data,list): return await i.followup.send('❌ 設定檔格式不完整。',ephemeral=True)
    try:
        for item in products:
            if not isinstance(item,dict) or not isinstance(item.get('product'),str): raise ValueError('商品資料格式錯誤')
            if int(item.get('price',0))<0 or int(item.get('stock',0))<0: raise ValueError('商品價格／庫存不能小於 0')
            if str(item.get('availability_status','正常提供')) not in AVAILABILITY: raise ValueError('商品供應狀態錯誤')
        for item in boost_prices_data:
            if not isinstance(item,dict) or str(item.get('tier')) not in BOOST_TIERS: raise ValueError('代肝額度格式錯誤')
            if int(item.get('price',0))<0: raise ValueError('代肝價格不能小於 0')
        for k in settings:
            if not isinstance(k,str) or not k or len(k)>100: raise ValueError('設定名稱格式錯誤')
        for item in balances_data:
            if not isinstance(item,dict) or not str(item.get('user_id','')).isdigit(): raise ValueError('會員餘額資料格式錯誤')
            if int(item.get('balance',0)) < 0: raise ValueError('會員餘額不能小於 0')
    except (TypeError,ValueError) as e:
        return await i.followup.send(f'❌ 設定檔驗證失敗：{e}',ephemeral=True)
    try:
        with db:
            cur=db.cursor(); prefix=f'g:{i.guild.id}:'
            for k,v in settings.items():
                if not isinstance(k,str) or not k or len(k)>100: continue
                cur.execute('INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',(prefix+k,str(v)))
            for item in products:
                if not isinstance(item,dict) or not item.get('product'): continue
                product=str(item['product'])[:100]; price_v=int(item.get('price',0) or 0); stock_v=int(item.get('stock',0) or 0); enabled_v=1 if int(item.get('enabled',1) or 0) else 0; availability_v=str(item.get('availability_status','正常提供'))
                if price_v<0 or stock_v<0 or availability_v not in AVAILABILITY: continue
                cur.execute('''INSERT INTO products(product,price,stock,enabled,availability_status) VALUES(?,?,?,?,?) ON CONFLICT(product) DO UPDATE SET price=excluded.price,stock=excluded.stock,enabled=excluded.enabled,availability_status=excluded.availability_status''',(product,price_v,stock_v,enabled_v,availability_v))
            for item in boost_prices_data:
                if not isinstance(item,dict) or not item.get('tier'): continue
                tier=str(item['tier']); price_v=int(item.get('price',0) or 0)
                if tier not in BOOST_TIERS or price_v<0: continue
                cur.execute('''INSERT INTO boost_prices(guild_id,tier,price) VALUES(?,?,?) ON CONFLICT(guild_id,tier) DO UPDATE SET price=excluded.price''',(str(i.guild.id),tier,price_v))
            for item in balances_data:
                if not isinstance(item,dict) or not str(item.get('user_id','')).isdigit(): continue
                user_id=str(item['user_id']); balance_v=int(item.get('balance',0) or 0)
                if balance_v < 0: continue
                cur.execute('INSERT INTO balances(user_id,balance) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET balance=excluded.balance',(user_id,balance_v))
            db.commit()
    except (ValueError,sqlite3.Error) as e: return await i.followup.send(f'❌ 匯入失敗，沒有完成設定更新：{e}',ephemeral=True)
    source_gid=str(data.get('source_guild_id',''))
    warnings=[]
    if source_gid and source_gid!=str(i.guild.id):
        id_keys=[k for k in settings if k.endswith('_id') or k.endswith('_ids')]
        if id_keys: warnings.append('⚠️ 來源伺服器與目前伺服器不同；頻道／身分組 ID 類設定可能需要重新設定。')
    await i.followup.send(f'✅ 設定匯入完成。\n已恢復：{len(settings)} 項文字設定、{len(products)} 個商品設定、{len(boost_prices_data)} 個代肝價格、{len(balances_data)} 筆會員餘額。\n' + ('\n'.join(warnings)+'\n' if warnings else '') + '\n⚠️ 訂單與交易紀錄不會被匯入；會員餘額會依匯入檔覆蓋同 ID 的餘額。',ephemeral=True)

# 指令名稱直接使用繁體中文；Discord CHAT_INPUT 指令支援 Unicode 名稱。


@bot.event
async def on_ready():
    global TICKET_SYNCED, VIEWS_REGISTERED
    if not TICKET_SYNCED:
        TICKET_CHANNEL_IDS.update(str(r['channel_id']) for r in q('SELECT channel_id FROM ticket_channels',(),True))
    if not VIEWS_REGISTERED:
        bot.add_view(OpenTicketView())
        bot.add_view(WelcomeCloseView())
        bot.add_view(ServiceTypeView())
        bot.add_view(CoinProductView())
        bot.add_view(TicketControlView())
        bot.add_view(ClosedTicketView())
        bot.add_view(BoostTierView())
        for r in q("SELECT id FROM orders WHERE status='待付款'",(),True):
            bot.add_view(PaymentMethodView(int(r['id'])))
            bot.add_view(PaymentView(int(r['id'])))
        for r in q("SELECT id FROM orders WHERE status IN ('待確認付款','待排單','待交貨','處理中','待收貨')",(),True):
            bot.add_view(OrderManageView(int(r['id'])))
        VIEWS_REGISTERED=True
    # 每次 ready 清理本 Bot 自己留下的舊互動面板；不碰其他 Bot。
    for g in bot.guilds:
        try:
            removed=await remove_obsolete_bot_panels(g)
            print(f'[PANEL_CLEANUP] {g.name} removed={removed}')
        except Exception as e:
            print('[PANEL_CLEANUP] 清理失敗:', repr(e))

    if not TICKET_SYNCED:
        sync_ok=True
        # 清除 Discord 上殘留的「全域」舊指令。指令已複製到各伺服器；
        # 若全域還留著舊版 /改名工單，輸入框會出現兩個同名指令，選到舊的就會跑出舊面板。
        try:
            await bot.http.bulk_upsert_global_commands(bot.application_id,[])
            print('[COMMAND_SYNC] 已清除全域舊指令')
        except Exception as e:
            print(f'[COMMAND_SYNC] 清除全域舊指令失敗｜error={e!r}')
        for g in bot.guilds:
            success=False
            for attempt in range(1,4):
                try:
                    # Slash commands are copied to each guild for immediate availability.
                    bot.tree.copy_global_to(guild=g)
                    synced = await bot.tree.sync(guild=g)
                    print(f'伺服器同步成功：{g.name} ({g.id})，{len(synced)} 個指令｜第 {attempt} 次')
                    success=True
                    break
                except (discord.HTTPException,discord.Forbidden,discord.NotFound) as e:
                    print(f'[COMMAND_SYNC] guild={g.id} attempt={attempt} failed｜error={e!r}')
                    if attempt < 3:
                        await asyncio.sleep(attempt * 1.5)
                except Exception as e:
                    print(f'[COMMAND_SYNC] guild={g.id} attempt={attempt} unexpected｜error={e!r}')
                    traceback.print_exc()
                    break
            if not success:
                sync_ok=False
        # 只在所有目前伺服器都成功時標記完成；失敗的下一次 on_ready 仍會重試。
        TICKET_SYNCED=sync_ok
    if not payment_reminder_loop.is_running(): payment_reminder_loop.start()
    for g in bot.guilds:
        try:
            me=g.me or g.get_member(bot.user.id)
            if me:
                p=me.guild_permissions
                print(f'[BOT_PERMS] guild={g.name}({g.id}) administrator={p.administrator} manage_guild={p.manage_guild} manage_channels={p.manage_channels} manage_roles={p.manage_roles} manage_messages={p.manage_messages} view_channel={p.view_channel} send_messages={p.send_messages}')
        except Exception as e:
            print(f'[BOT_PERMS] guild={getattr(g,"id",None)} check failed: {e!r}')
    print('登入：',bot.user,'servers',len(bot.guilds))


# ---- 設定匯出／匯入 ----
CONFIG_EXPORT_VERSION = 2


def build_config_export(guild_id):
    settings_rows = q('SELECT key,value FROM settings WHERE key LIKE ?', (f'g:{guild_id}:%',), True)
    settings = {}
    prefix = f'g:{guild_id}:'
    for row in settings_rows:
        settings[row['key'][len(prefix):]] = row['value']
    product_rows = q('SELECT product,price,stock,enabled,availability_status FROM products ORDER BY product', (), True)
    products = [dict(r) for r in product_rows]
    boost_rows = q('SELECT tier,price FROM boost_prices WHERE guild_id=? ORDER BY tier', (str(guild_id),), True)
    boost_prices_data = [dict(r) for r in boost_rows]
    balance_rows = q('SELECT user_id,balance FROM balances ORDER BY user_id', (), True)
    balances_data = [dict(r) for r in balance_rows]
    return {'format':'CT小舖設定檔','version':CONFIG_EXPORT_VERSION,'exported_at':now(),'source_guild_id':str(guild_id),'settings':settings,'products':products,'boost_prices':boost_prices_data,'balances':balances_data,'meta':{'bot_config_schema':3,'notes':'Discord 頻道／身分組 ID 會隨伺服器不同而失效；會員餘額會一併匯出／匯入。'}}


def config_bytes(guild_id):
    return json.dumps(build_config_export(guild_id), ensure_ascii=False, indent=2).encode('utf-8')


# 指令名稱直接使用繁體中文；Discord CHAT_INPUT 指令支援 Unicode 名稱。

@bot.tree.error
async def tree_error(i,e):
    # V75：統一處理全部 Slash Command 例外，避免只顯示「權限與參數」而看不到真正原因。
    command_name=getattr(getattr(i,'command',None),'qualified_name',None) or '未知指令'
    root=e.original if isinstance(e,app_commands.CommandInvokeError) and getattr(e,'original',None) else e
    print(f'[COMMAND_ERROR] command={command_name!r}｜type={type(root).__name__}｜error={root!r}')
    traceback.print_exception(type(root), root, root.__traceback__)

    if isinstance(root,app_commands.MissingPermissions):
        missing=', '.join(getattr(root,'missing_permissions',[]) or [])
        msg=f'❌ `/{command_name}` 使用權限不足。' + (f' Discord 要求：`{missing}`。' if missing else ' 請確認你有管理員身分組或必要的 Discord 管理權限。')
    elif isinstance(root,app_commands.CheckFailure):
        msg=f'❌ `/{command_name}` 目前沒有使用權限。'
    elif isinstance(root,app_commands.TransformerError):
        msg=f'❌ `/{command_name}` 的參數格式不正確，請重新選擇／輸入。'
    elif isinstance(root,discord.Forbidden):
        msg=f'❌ `/{command_name}` 被 Discord 拒絕：機器人缺少對應頻道／訊息／管理頻道權限。'
    elif isinstance(root,discord.NotFound):
        msg=f'❌ `/{command_name}` 找不到指定的 Discord 頻道、訊息或分類，可能已被刪除。'
    elif isinstance(root,discord.HTTPException):
        msg=f'❌ `/{command_name}` Discord API 暫時失敗（HTTP {getattr(root,"status","?")}）。請稍後再試。'
    elif isinstance(root,sqlite3.Error):
        msg=f'❌ `/{command_name}` 資料庫操作失敗：`{str(root)[:220]}`\n請看 Railway Log 的 `[COMMAND_ERROR]`。'
    elif isinstance(root,TypeError):
        msg=f'❌ `/{command_name}` 參數型別發生錯誤：`{str(root)[:220]}`\n請看 Railway Log 的 `[COMMAND_ERROR]`。'
    elif isinstance(root,AttributeError):
        msg=f'❌ `/{command_name}` 讀取資料時發生錯誤：`{str(root)[:220]}`\n請看 Railway Log 的 `[COMMAND_ERROR]`。'
    else:
        msg=f'❌ `/{command_name}` 執行失敗：`{type(root).__name__}: {str(root)[:220]}`\n請看 Railway Log 的 `[COMMAND_ERROR]`。'
    try:
        if i.response.is_done(): await i.followup.send(msg,ephemeral=True)
        else: await i.response.send_message(msg,ephemeral=True)
    except (discord.HTTPException,discord.NotFound):
        pass

if __name__=='__main__':
    if not TOKEN: raise SystemExit('請設定 DISCORD_TOKEN 環境變數。')
    bot.run(TOKEN)

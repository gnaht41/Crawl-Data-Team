import scrapy
import pymongo


class GoodreadsSpider(scrapy.Spider):
    name = 'goodreads_spider'
    allowed_domains = ['goodreads.com']
    start_urls = [
        'https://www.goodreads.com/list/show/1.Best_Books_Ever?page=1']
    count = 0  # Đếm số dòng dữ liệu
    limit = 1001  # Giới hạn là 1000 dòng dữ liệu

    def __init__(self, *args, **kwargs):
        super(GoodreadsSpider, self).__init__(*args, **kwargs)

        # Thiết lập kết nối đến MongoDB
        self.client = pymongo.MongoClient(
            'localhost', 27017)
        self.db = self.client['goodreads_db']
        self.collection = self.db['books']

    def parse(self, response):
        books = response.css('tr[itemtype="http://schema.org/Book"]')

        for book in books:
            if self.count >= self.limit:
                return  # Dừng khi đủ 1000 dòng dữ liệu

            rank = book.css('td.number::text').get()
            title = book.css('a.bookTitle span::text').get()
            author = book.css('a.authorName span::text').get()

            score_text = book.css('span.smallText a::text').re_first(
                r'score: ([\d,]+)')
            score = score_text if score_text else 'Not Available'

            book_link = response.urljoin(
                book.css('a.bookTitle::attr(href)').get())
            yield scrapy.Request(book_link, callback=self.parse_book_details, meta={
                'rank': rank,
                'title': title,
                'author': author,
                'score': score
            })

            self.count += 1  # Tăng số lượng dòng đã cào

        # Phân trang - Tìm liên kết đến trang tiếp theo
        next_page = response.css(
            'div.pagination a.next_page::attr(href)').get()
        if next_page and self.count < self.limit:
            next_page_url = response.urljoin(next_page)
            yield scrapy.Request(next_page_url, callback=self.parse)

    def parse_book_details(self, response):
        rank = response.meta['rank']
        title = response.meta['title']
        author = response.meta['author']
        score = response.meta['score']

        rating = response.css('div.RatingStatistics__rating::text').get()
        number_of_ratings = response.css(
            'span[data-testid="ratingsCount"]::text').re_first(r'([\d,]+)')
        date = response.css('p[data-testid="publicationInfo"]::text').get()
        description = response.css(
            'div.DetailsLayoutRightParagraph__widthConstrained span.Formatted::text').get()
        reviews = response.css(
            'span[data-testid="reviewsCount"]::text').re_first(r'([\d,]+)')

        # Xử lý page format tách ra làm 2 cột
        page_format = response.css('p[data-testid="pagesFormat"]::text').get()
        if page_format:
            pages, cover_type = page_format.split(',', 1)
            pages = pages.split()[0].strip()  # Chỉ lấy số trang
            cover_type = cover_type.strip()  # Loại bìa
        else:
            pages = None
            cover_type = None

        book_data = {
            'Rank': rank,             # Thứ tự
            'Title': title,           # Tên sách
            'Author': author,         # Tác giả
            'Rating': rating,         # Đánh giá trung bình
            'Number of Ratings': number_of_ratings,  # Số lượng đánh giá
            'Date': date,             # Ngày xuất bản
            'Description': description,  # Mô tả nội dung
            'Reviews': reviews,       # Số lượng bình luận
            'Pages': pages,           # Số trang
            'Cover Type': cover_type,  # Loại bìa
            'Score': score            # Điểm số
        }

        # Lưu dữ liệu vào MongoDB
        self.collection.insert_one(book_data)

    def close(self, reason):
        # Đóng kết nối MongoDB khi kết thúc cào dữ liệu
        self.client.close()

import os
import shutil
import json
import datetime
import uuid
from typing import List, Dict, Any

class OperationLogger:
    def __init__(self):
        self.log_dir = "operation_logs"
        self.ensure_log_directory()
        
    def ensure_log_directory(self):
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)
            
    def create_operation_log(self) -> str:
        """새로운 작업 로그 생성"""
        operation_id = str(uuid.uuid4())
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = f"operations.json"
        
        if os.path.exists(os.path.join(self.log_dir, log_file)):
            with open(os.path.join(self.log_dir, log_file), 'r') as f:
                try:
                    log_data = json.load(f)
                except json.JSONDecodeError:
                    log_data = {"operations": {}, "timestamps": {}}
        else:
            log_data = {"operations": {}, "timestamps": {}}

        # operations와 timestamps 모두에 데이터 저장
        operation_data = {
            "id": operation_id,
            "timestamp": timestamp,
            "moves": [],
            "status": "started"
        }
        
        log_data["operations"][operation_id] = operation_data
        log_data["timestamps"][timestamp] = operation_id
        
        self.save_log(log_file, log_data)
        return operation_id
        
    def log_file_move(self, operation_id: str, source: str, destination: str):
        """파일 이동 기록"""
        log_file = self.get_log_file(operation_id)
        if log_file:
            with open(os.path.join(self.log_dir, log_file), 'r+') as f:
                log_data = json.load(f)
                log_data["moves"].append({
                    "source": source,
                    "destination": destination,
                    "timestamp": datetime.datetime.now().isoformat()
                })
                f.seek(0)
                json.dump(log_data, f, indent=2)
                f.truncate()

    def complete_operation(self, operation_id: str):
        """작업 완료 표시"""
        log_file = self.get_log_file(operation_id)
        if log_file:
            with open(os.path.join(self.log_dir, log_file), 'r+') as f:
                log_data = json.load(f)
                log_data["status"] = "completed"
                f.seek(0)
                json.dump(log_data, f, indent=2)
                f.truncate()

    def get_log_file(self, operation_id: str) -> str:
        """작업 ID로 로그 파일 찾기"""
        for file in os.listdir(self.log_dir):
            if operation_id in file:
                return file
        return None

    def save_log(self, filename: str, data: Dict[str, Any]):
        """로그 데이터 저장"""
        with open(os.path.join(self.log_dir, filename), 'w') as f:
            json.dump(data, f, indent=2)

    def get_operation_by_id(self, operation_id: str) -> Dict:
        """ID로 작업 검색"""
        with open(os.path.join(self.log_dir, "operations.json"), 'r') as f:
            log_data = json.load(f)
            return log_data["operations"].get(operation_id)

    def get_operation_by_timestamp(self, timestamp: str) -> Dict:
        """타임스탬프로 작업 검색"""
        with open(os.path.join(self.log_dir, "operations.json"), 'r') as f:
            log_data = json.load(f)
            operation_id = log_data["timestamps"].get(timestamp)
            if operation_id:
                return log_data["operations"].get(operation_id)
            return None

    def list_recent_operations(self, limit: int = 10) -> List[Dict]:
        """최근 작업 목록 조회"""
        with open(os.path.join(self.log_dir, "operations.json"), 'r') as f:
            log_data = json.load(f)
            timestamps = sorted(log_data["timestamps"].keys(), reverse=True)
            recent = []
            for ts in timestamps[:limit]:
                operation_id = log_data["timestamps"][ts]
                recent.append(log_data["operations"][operation_id])
            return recent

class FolderWizard:
    def __init__(self):
        self.logger = OperationLogger()
        self.current_operation_id = None

    def rollback_operation(self, operation_id: str) -> bool:
        """작업 롤백"""
        try:
            log_file = self.logger.get_log_file(operation_id)
            if not log_file:
                print(f"작업 ID {operation_id}를 찾을 수 없습니다.")
                return False

            with open(os.path.join(self.logger.log_dir, log_file), 'r') as f:
                log_data = json.load(f)

            if log_data["status"] != "completed":
                print("완료되지 않은 작업은 롤백할 수 없습니다.")
                return False

            # 역순으로 파일 이동 실행
            for move in reversed(log_data["moves"]):
                source = move["destination"]
                destination = move["source"]
                
                if os.path.exists(source):
                    os.makedirs(os.path.dirname(destination), exist_ok=True)
                    shutil.move(source, destination)
                    print(f"롤백: {source} -> {destination}")

            print(f"작업 {operation_id} 롤백 완료")
            return True

        except Exception as e:
            print(f"롤백 중 오류 발생: {e}")
            return False

    def process_down_movement(self, path: str, delimiters: List[str], chars_to_remove: List[str]) -> None:
        self.current_operation_id = self.logger.create_operation_log()
        
        try:
            for root, _, files in os.walk(path):
                for file in files:
                    current_path = os.path.join(root, file)
                    filename = os.path.splitext(file)[0]
                    extension = os.path.splitext(file)[1]
                    
                    processed_name = self.remove_chars(filename, chars_to_remove)
                    
                    parts = [processed_name]
                    for delimiter in delimiters:
                        new_parts = []
                        for part in parts:
                            new_parts.extend(part.split(delimiter))
                        parts = new_parts
                    
                    new_path = root
                    for part in parts[:-1]:
                        new_path = os.path.join(new_path, part)
                        os.makedirs(new_path, exist_ok=True)
                    
                    new_filename = parts[-1] + extension
                    target_path = os.path.join(new_path, new_filename)
                    if current_path != target_path:
                        shutil.move(current_path, target_path)
                        self.logger.log_file_move(self.current_operation_id, current_path, target_path)
                        print(f"이동됨: {current_path} -> {target_path}")

            self.logger.complete_operation(self.current_operation_id)
            print(f"작업 ID: {self.current_operation_id}")
            
        except Exception as e:
            print(f"오류 발생: {e}")
            self.rollback_operation(self.current_operation_id)
            raise

    def main_menu(self) -> None:
        while True:
            print("\n=== 폴더 마법사 ===")
            print("1. 폴더 구조 상향 이동")
            print("2. 폴더 구조 하향 이동")
            print("3. 작업 이력 보기")
            print("4. 작업 롤백")
            print("5. 종료")
            
            choice = input("\n선택하세요: ")
            
            if choice == "1":
                self.move_up_structure()
            elif choice == "2":
                self.move_down_structure()
            elif choice == "3":
                self.show_operation_history()
            elif choice == "4":
                self.handle_rollback()
            elif choice == "5":
                print("프로그램을 종료합니다.")
                break

    def get_path_input(self) -> str:
        path = input("대상 경로를 입력하세요: ").strip()
        if not os.path.exists(path):
            raise ValueError("존재하지 않는 경로입니다.")
        return path

    def move_up_structure(self) -> None:
        try:
            path = self.get_path_input()
            levels = int(input("상위로 이동할 레벨을 입력하세요: "))
            delete_empty = input("빈 폴더를 삭제할까요? (y/n): ").lower() == 'y'
            
            # 상위 이동 로직 구현
            self.process_up_movement(path, levels, delete_empty)
            print("상향 이동이 완료되었습니다.")
            
        except Exception as e:
            print(f"오류 발생: {e}")

    def move_down_structure(self) -> None:
        try:
            path = self.get_path_input()
            delimiters = input("구분자들을 입력하세요 (쉼표로 구분): ").split(',')
            chars_to_remove = input("제거할 문자들을 입력하세요 (쉼표로 구분, 생략 가능): ").split(',')
            
            # 하위 이동 로직 구현
            self.process_down_movement(path, delimiters, chars_to_remove)
            print("하향 이동이 완료되었습니다.")
            
        except Exception as e:
            print(f"오류 발생: {e}")

    def process_up_movement(self, path: str, levels: int, delete_empty: bool) -> None:
        for root, dirs, files in os.walk(path, topdown=False):
            for file in files:
                current_path = os.path.join(root, file)
                filename = os.path.splitext(file)[0]
                extension = os.path.splitext(file)[1]
                
                target_dir = root
                for _ in range(levels):
                    target_dir = os.path.dirname(target_dir)
                
                target_path = os.path.join(target_dir, filename + extension)
                
                # 중복 파일명 처리
                counter = 1
                while os.path.exists(target_path):
                    base_name = filename + f"_{counter}"
                    target_path = os.path.join(target_dir, base_name + extension)
                    counter += 1
                
                shutil.move(current_path, target_path)
                print(f"이동됨: {current_path} -> {target_path}")

        if delete_empty:
            self.remove_empty_folders(path)

    @staticmethod
    def remove_chars(filename: str, chars_to_remove: List[str]) -> str:
        for char in chars_to_remove:
            if char:  # 빈 문자열이 아닌 경우에만 처리
                filename = filename.replace(char.strip(), '')
        return filename

    @staticmethod
    def remove_empty_folders(path: str) -> None:
        for root, dirs, files in os.walk(path, topdown=False):
            for dir_name in dirs:
                dir_path = os.path.join(root, dir_name)
                try:
                    if not os.listdir(dir_path):  # 폴더가 비어있는 경우
                        os.rmdir(dir_path)
                except OSError:
                    continue

    def show_operation_history(self):
        """최근 작업 이력 표시"""
        recent_ops = self.logger.list_recent_operations(10)  # 최근 10개 작업
        if not recent_ops:
            print("작업 이력이 없습니다.")
            return

        print("\n=== 최근 작업 이력 ===")
        for op in recent_ops:
            status_emoji = "✅" if op['status'] == "completed" else "🔄"
            print(f"{status_emoji} [{op['timestamp']}] ID: {op['id']}")
            print(f"   처리된 파일: {len(op['moves'])}개")
            print("-" * 50)

    def handle_rollback(self):
        """롤백 처리"""
        print("\n=== 롤백 실행 ===")
        print("1. 작업 ID로 롤백")
        print("2. 시간으로 롤백")
        
        choice = input("\n선택하세요: ")
        
        if choice == "1":
            operation_id = input("롤백할 작업 ID를 입력하세요: ")
            self.rollback_operation(operation_id)
        elif choice == "2":
            timestamp = input("롤백할 작업 시간을 입력하세요 (YYYYMMDD_HHMMSS 형식): ")
            operation = self.logger.get_operation_by_timestamp(timestamp)
            if operation:
                self.rollback_operation(operation['id'])
            else:
                print("해당 시간의 작업을 찾을 수 없습니다.")

if __name__ == "__main__":
    wizard = FolderWizard()
    wizard.main_menu()